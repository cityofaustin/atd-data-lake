'''
json standardization for bluetooth
Author: Nadia Florez
'''
import os
from json import dumps
import datetime
import tempfile
import shutil
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import gc

import pandas as pd
from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import last_upd_cat, call_knack_access, config
from util import date_util

PROGRAM_DESC = "Performs JSON canonicalization for Bluetooth data between the raw and rawjson Data Lake buckets"

"Number of months to go back for filing records"
DATE_EARLIEST = 12

"S3 bucket as source"
SRC_BUCKET = "atd-data-lake-raw"

"S3 bucket to target"
TGT_BUCKET = "atd-data-lake-rawjson"

"Temporary directory holding-place"
_TEMP_DIR = None

"S3 object"
_S3 = None

class BT_JSON_Standard:
    ''' 'Class standardizes bluetooth directory data into json'''

    def __init__(self, identifier, storagePath, collection_date, fileType, catalog):

        self.identifier = identifier
        self.srcStoragePath = storagePath
        self.tgtStoragePath = storagePath[:-4] + ".json" # TODO: Maybe let storagePath just be the S3 path: use identifier.
        # TODO: Provide standardized method to reconstruct the S3 path.
        self.collection_date = str(collection_date)
        self.processing_date = str(date_util.localize(arrow.now().datetime))
        self.fileType = fileType
        self.columns, self.dateColumns = self.set_data_columns()
        self.json_header_template = self.set_json_header_template()
        self.catalog = catalog

    def set_data_columns(self):
        if self.fileType == "unmatched":
            bt_data_columns = ["host_timestamp", "ip_address", "field_timestamp",
                           "reader_id", "dev_addr"]
            btDateColumns = (["host_timestamp", "field_timestamp"], self._parseTime)
        elif self.fileType == "matched":
            bt_data_columns = ["dev_addr", "origin_reader_id", "dest_reader_id",
                            "start_time", "end_time", "travel_time_secs", "speed",
                            "match_validity", "filter_id"]
            btDateColumns = (["start_time", "end_time"], self._parseTime)
        elif self.fileType == "traf_match_summary":
            bt_data_columns = ["origin_reader_id", "dest_reader_id", "origin_road", "origin_cross_st",
                               "origin_dir", "dest_road", "dest_cross_st", "dest_dir", "seg_length",
                               "timestamp", "avg_travel_time", "avg_speed", "interval", "samples",
                               "std_dev"]
            btDateColumns = (["timestamp"], self._parseTimeShort)
        return bt_data_columns, btDateColumns

    def set_json_header_template(self):

        json_header_template = {"data_type": "bluetooth",
                                "file_type": self.fileType,
                                "origin_filename": self.identifier + ".txt",
                                "target_filename": self.identifier + ".json",
                                "collection_date": self.collection_date,
                                "processing_date": self.processing_date}
        return json_header_template

    @staticmethod
    def _parseTime(inTime):
        "Parses the time string as encountered in the Bluetooth source files."
        
        try:
            return str(date_util.localize(datetime.datetime.strptime(inTime, "%m/%d/%Y %I:%M:%S %p")))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parseTimeShort(inTime):
        "Parses the time string as encountered in the Bluetooth source files."
        
        try:
            return str(date_util.localize(datetime.datetime.strptime(inTime, "%m/%d/%Y %I:%M %p")))
        except (ValueError, TypeError):
            return None

    def jsonize(self):

        #initiate json object
        json_data = {'header': self.json_header_template, 'data': None}
        #call bucket
        #print("--0: Pull from bucket...")
        fullPathR = os.path.join(_TEMP_DIR, self.identifier + ".csv")
        _S3.Bucket(SRC_BUCKET).download_file(self.srcStoragePath, fullPathR)
        # add data array of objects from rows of dataframe
        #print("--1: Read CSV...")
        data = pd.read_csv(fullPathR, header=None, names=self.columns)
        
        # Unwrap to dictionary:
        #print("--2: Convert to dictionary...")
        json_data['data'] = data.to_dict(orient="records")
        
        # Convert timestamps to the canonicalized, time zone-aware:
        #print("--3: Convert timestamps...")
        for item in json_data['data']:
            for col in self.dateColumns[0]:
                item[col] = self.dateColumns[1](item[col])

        ##write to s3 raw json bucket
        #print("--4: Write out...")
        fullPathW = os.path.join(_TEMP_DIR, self.identifier + ".json")
        with open(fullPathW, 'w') as bt_json_file:
            bt_json_file.write(dumps(json_data))

        #print("--5: Load to S3...")
        with open(fullPathW, 'rb') as bt_json_file:
            s3Object = _S3.Object(TGT_BUCKET, self.tgtStoragePath)
            s3Object.put(Body=bt_json_file)

        # Clean up:
        #print("--6: Clean up...")
        os.remove(fullPathR)
        os.remove(fullPathW)

    def to_catalog(self):

        catalog = self.catalog
        identifier = self.identifier
        pointer = self.tgtStoragePath
        collection_date = self.collection_date
        processing_date = self.processing_date
        json_blob = self.json_header_template

        metadata = {"repository": 'rawjson', "data_source": 'bt',
                    "identifier": identifier, "pointer": pointer,
                    "collection_date": collection_date,
                    "processing_date": processing_date, "metadata": json_blob}

        catalog.upsert(metadata)

def main():
    "Main entry-point that takes --last_run_date parameter"
    
    global _TEMP_DIR
    global _S3
    
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("bt_json_standard: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            monthsOld = int(args.months_old)
        except ValueError:
            monthsOld = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        monthsOld = DATE_EARLIEST
    
    # Prepare to make a copy of Knack dependency:
    call_knack_access.insert_units_to_bucket2("bt", sameDay=args.same_day)
    
    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    _S3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    _TEMP_DIR = tempfile.mkdtemp()
    print("Created holding place: %s" % _TEMP_DIR)
    
    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    lastUpdateWorker = last_upd_cat.LastUpdateCat("raw", "rawjson", "bt", monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=False):
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
        worker = BT_JSON_Standard(record.identifier, record.s3Path, record.fileDate, fileType, catalog)
        worker.jsonize()
        worker.to_catalog()
        
        # Clean up:
        del worker
        gc.collect()
        
        # Increment count:
        count += 1
    
    # Clean up the temporary output directory:
    shutil.rmtree(_TEMP_DIR)

    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
