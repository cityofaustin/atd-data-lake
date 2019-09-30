'''
CoA Data Lake 'ready' bucket for bluetooth
Author: Nadia Florez
'''

import json
import os
import hashlib
import tempfile
import shutil
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import gc

import pandas as pd
from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import last_upd_cat, unit_data, config
from util import date_util

PROGRAM_DESC = "Performs JSON enrichment for Bluetooth data between the 'rawjson' and 'ready' Data Lake buckets"

"Number of months to go back for filing records"
DATE_EARLIEST = 12

"S3 bucket as source"
SRC_BUCKET = "atd-data-lake-rawjson"

"S3 bucket to target"
TGT_BUCKET = "atd-data-lake-ready"

class BT_Ready:

    '''Class creates 'ready' bucket for bluetooth data from bt json data
    and bt device json information
    '''

    def __init__(self, identifier, storagePath, data_json, device_json, fileType, catalog):

        ##data_json and device_json should be dicts

        self.identifier = identifier
        self.tgtStoragePath = storagePath
        self.processing_date = str(date_util.localize(arrow.now().datetime))
        self.jheader = data_json['header']
        self.jdata = data_json['data']
        self.jdevices = device_json['devices']
        self.fileType = fileType
        self.catalog = catalog

    def edit_header(self):

        header = self.jheader
        header['processing_date'] = self.processing_date

        return header

    def create_hash(self, row):

        device_type = row['device_type']
        device_ip = row['device_ip']
        lat = row['lat']
        lon = row['lon']
        to_hash = device_type+device_ip+str(lat)+str(lon)

        h = hashlib.md5()
        h.update(bytes(to_hash, "utf-8"))

        return(h.hexdigest())

    def hash_devices(self, devices_df):

        hashed_devices = devices_df.copy()
        hashed_devices['device_id'] = hashed_devices.apply(self.create_hash, axis=1)

        return hashed_devices

    def df_to_json(self, df):

        return df.apply(lambda x: x.to_dict(), axis=1).tolist()

    def jsonize(self):

        data_no_hash = pd.DataFrame(self.jdata)
        devices_no_hash = pd.DataFrame(self.jdevices)

        devices = self.hash_devices(devices_no_hash)
        """
        data = (data_no_hash.merge(devices[['device_name', 'device_id']],
                                  left_on='reader_id', right_on='device_name',
                                  how='right')
                .drop(columns='device_name')) ##may want to drop reader_id instead
        """
        if self.fileType == "unmatched":
            data = (data_no_hash.merge(devices[['device_name', 'device_id']],
                                       left_on='reader_id', right_on='device_name', how='inner')
                                       .drop(columns='device_name'))
            # TODO: Consider removing "reader_id" here, for memory efficiency.
            jdevices = self.df_to_json(devices[devices.device_id.isin(data.device_id.unique())])
        elif self.fileType == "matched" or self.fileType == "traf_match_summary":
            data = (data_no_hash.merge(devices[['device_name', 'device_id']],
                                       left_on='origin_reader_id', right_on='device_name', how='inner')
                                       .drop(columns='device_name').rename(columns={"device_id": "origin_device_id"}))
            data = (data.merge(devices[['device_name', 'device_id']],
                                       left_on='dest_reader_id', right_on='device_name', how='inner')
                                       .drop(columns='device_name').rename(columns={"device_id": "dest_device_id"}))
            # TODO: Consider removing "origin_reader_id" and "dest_reader_id" here, for memory efficiency.
            jdevices = self.df_to_json(devices[devices.device_id.isin(data.origin_device_id
                                        .append(data.dest_device_id, ignore_index=True).unique())])
        else:
            raise("Invalid file type '%s'" % self.fileType)

        jdata = self.df_to_json(data)

        jsonized = {'header': self.edit_header(),
                    'data': jdata,
                    'devices': jdevices}

        return(jsonized) ##then to file or catalog!

    def to_catalog(self):

        metadata = {"repository": 'ready', "data_source": 'bt',
                    "identifier": self.identifier, "pointer": self.tgtStoragePath,
                    "collection_date": self.jheader["collection_date"],
                    "processing_date": self.processing_date, "metadata": self.edit_header()}

        self.catalog.upsert(metadata)

def main():
    "Main entry-point that takes --last_run_date parameter"

    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-M", "--missing", action="store_true", default=False, help="check for missing entries after the earliest processing date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("bt_ready: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            monthsOld = int(args.months_old)
        except ValueError:
            monthsOld = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        monthsOld = DATE_EARLIEST

    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    s3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    tempDir = tempfile.mkdtemp()
    print("Created holding place: %s" % tempDir)
    
    # Set up object for retrieving the Unit Data files:
    unitData = unit_data.UnitData(catalog, s3, SRC_BUCKET, "rawjson", "bt")

    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    lastUpdateWorker = last_upd_cat.LastUpdateCat("rawjson", "ready", "bt", monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        if record.identifier.startswith("unit_data"):
            # TODO: Figure out a better way to deal with avoiding unit_data in LastUpdateCat work.
            continue
        # TODO: We need to distinguish different file types. Use the base/ext thing later on to do this better.
        fileType = None
        if "_bt_summary_15_" in record.identifier:
            fileType = "traf_match_summary"
        elif "_btmatch_" in record.identifier:
            fileType = "matched"
        elif "_bt_" in record.identifier:
            fileType = "unmatched"
        else:
            continue
        print("%s: %s -> %s%s" % (record.s3Path, SRC_BUCKET, TGT_BUCKET, "" if not record.missingFlag else " (missing)"))

        # Retrieve the canonicalized JSON file:
        fullPathR = os.path.join(tempDir, record.identifier + ".json")
        s3.Bucket(SRC_BUCKET).download_file(record.s3Path, fullPathR)
        with open(fullPathR, 'r') as dataJSONFile:
            dataJSON = json.load(dataJSONFile)
        
        # Clean up the canonicalized JSON file:
        try:
            os.remove(fullPathR)
        except FileNotFoundError:
            pass
        
        # Retrieve respective Unit Data file:
        deviceJSON = unitData.getUnitData(record.fileDate)

        # Perform transformation:
        worker = BT_Ready(record.identifier, record.s3Path, dataJSON, deviceJSON, fileType, catalog)
        del dataJSON
        jsonData = worker.jsonize()

        # Write contents to S3:
        fullPathW = os.path.join(tempDir, record.identifier + ".json")
        # TODO: Currently we overwrite the temp canonicalized JSON file. That's okay, but we may want to consider a name change.
        with open(fullPathW, 'w') as bt_json_file:
            json.dump(jsonData, bt_json_file)
        del jsonData

        with open(fullPathW, 'rb') as bt_json_file:
            s3Object = s3.Object(TGT_BUCKET, record.s3Path)
            s3Object.put(Body=bt_json_file)
        
        # Clean up the "ready" file:
        try:
            os.remove(fullPathW)
        except FileNotFoundError:
            pass

        worker.to_catalog()
        del worker
        gc.collect() # Really try to free memory

        # Increment count:
        count += 1

    # Clean up the temporary output directory:
    shutil.rmtree(tempDir)

    print("Records processed: %d" % count)
    return count

if __name__ == "__main__":
    main()
