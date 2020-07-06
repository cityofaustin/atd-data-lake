'''
json standardization for GRIDSMART
Author: Nadia Florez
'''
import os
from json import dumps
import tempfile
import shutil
import datetime
import json
from argparse import ArgumentParser, RawDescriptionHelpFormatter

import pandas as pd
from pypgrest import Postgrest
import arrow
import pytz

import _setpath
from aws_transport.support import last_upd_cat, gs_investigate, config
from util import date_util

PROGRAM_DESC = "Performs JSON canonicalization for GRIDSMART data between the raw and rawjson Data Lake buckets"

"Number of months to go back for filing records"
DATE_EARLIEST = 12

"S3 bucket as source"
SRC_BUCKET = config.composeBucket("raw")

"S3 bucket to target"
TGT_BUCKET = config.composeBucket("rawjson")

"Temporary directory holding-place"
_TEMP_DIR = None

"S3 object"
_S3 = None

class GS_JSON_Standard:
    '''Class standardizes GRIDMSMART directory data into json,
    maintains file per guid'''

    def __init__(self, idBase, storagePath, collection_date, siteFile, catalog):

        ##csv paths should be a directory with guid as key and corresponding
        ##csv file path as value

        self.idBase = idBase
        self.storagePath = storagePath
        
        self.collection_date = str(collection_date)
        self.processing_date = str(date_util.localize(arrow.now().datetime))
        self.api_version = None
        self.columns = None
        self.json_header_template = self.set_json_header_template()
        self.siteFile = siteFile
        self.catalog = catalog
        
    @staticmethod
    def get_api_version(file_dict):

        csv_path0 = next(iter(file_dict.values()))

        api_version = int(pd.read_csv(csv_path0,
                                      header=None, usecols=[0],
                                      squeeze=True).unique()[0])
        return api_version

    def set_data_columns(self):
        if self.api_version == 8:
            self.columns = ["count_version", "site_version", "timestamp",
                            "utc_offset", "turn", "vehicle_length", "speed",
                            "light_state", "seconds_in_zone",
                            "vehicles_in_zone", "light_state_sec",
                            "sec_since_green", "zone_freeflow_speed",
                            "zone_freeflow_speed_cal"]
        elif self.api_version == 7:
            self.columns = ["count_version", "site_version", "timestamp",
                            "utc_offset", "turn", "vehicle_length", "speed",
                            "light_state", "seconds_in_zone",
                            "vehicles_in_zone", "confidence"]
        elif self.api_version == 4:
            self.columns = ["count_version", "site_version",  "timestamp",
                            "internal_veh_id", "internal_veh_type",
                            "vehicle_length", "speed", "turn", "allowable_turns",
                            "seconds_in_zone", "seconds_since_last_exit",
                            "queue_length", "light_state_on_exit",
                            "sec_since_green", "internal_frame_count", "day_night"]
        else:
            raise Exception("GRIDSMART counts file format %d is not supported." % self.api_version)

    def set_json_header_template(self):

        json_header_template = {"data_type": "gridsmart",
                                "zip_name": self.storagePath.split("/")[-1], # TODO: Use S3 abstraction.
                                "origin_filename": None,
                                "target_filename": None,
                                "collection_date": self.collection_date,
                                "processing_date": self.processing_date,
                                "version": self.api_version,
                                "guid": None}
        #Note: None values will be replaced in a per guid file basis
        return json_header_template

    def jsonize(self):

        # Read the .ZIP file and unpack here.
        fullPathR = os.path.join(_TEMP_DIR, self.idBase + ".zip")
        _S3.Bucket(SRC_BUCKET).download_file(self.storagePath, fullPathR)
        if not gs_investigate.investigate(fullPathR, lambda file_dict: self._jsonize_work(file_dict)):
            print("File %s not processed." % fullPathR)
            return False
        return True
    
    @staticmethod
    def _getTime(gsTimeString, tzInfo=None):
        """
        Parses the GRIDSMART-formatted time string and returns as UTC time, or given time zone.
        
        @param gsTimeString Time printed in format provided by GRIDSMART devices
        @param tzInfo Optional parenthesized time zone information-- e.g. "(GMT-06:00)"
        """
        if tzInfo:
            tzInfo = tzInfo.replace(":", "")
            return datetime.datetime.strptime(gsTimeString + " " + tzInfo, "%m/%d/%Y %I:%M:%S %p (%Z%z)")
        else:
            return pytz.utc.localize(datetime.datetime.strptime(gsTimeString, "%m/%d/%Y %I:%M:%S %p"))
        
    def _jsonize_work(self, file_dict):
        
        # TODO: Add in device metadata file support.

        n = len(file_dict)
        i = 0
        self.api_version = self.get_api_version(file_dict)
        self.set_data_columns()
        catCache = []
        for key, value in file_dict.items():

            guid = key
            csv_path = value #recall this is a temporary location from unzipped
            target_filename = self.idBase + '_' + guid + "_" + self.collection_date.split()[0] + '.json'
            target_path = os.path.dirname(self.storagePath) + "/" + target_filename #ToDo: think about jsonized gs file structure

            print(("Working on file {}").format(csv_path))
            #initiate json object
            json_data = {'header': self.json_header_template, 'data': None}
            #add header information
            json_data['header']['origin_filename'] = guid + '.csv'
            json_data['header']['target_filename'] = target_filename
            json_data['header']['version'] = self.api_version
            json_data['header']['guid'] = guid

            data = pd.read_csv(csv_path, header=None, names=self.columns)
            json_data['data'] = data.apply(lambda x: x.to_dict(), axis=1).tolist()

            # Fix the time representation. First, find the time delta:
            try:
                hostTimeUTC = self._getTime(self.siteFile["datetime"]["HostTimeUTC"])
                deviceTime = self._getTime(self.siteFile["datetime"]["DateTime"], self.siteFile["datetime"]["TimeZoneId"].split()[0])
                timeDelta = hostTimeUTC - deviceTime
                
                # At this point, collect an indication of whether this file accounts for some of the previous day, or some of the
                # next day.
                collDatetime = arrow.get(self.collection_date).datetime
                collDatetime = collDatetime.replace(hour=0, minute=0, second=0, microsecond=0)
                timestamp = None
                if self.api_version == 8 and json_data['data']:
                    timestamp = datetime.datetime.strptime(self.collection_date.split()[0] + " 000000", "%Y-%m-%d %H%M%S")
                    timestamp -= datetime.timedelta(minutes=json_data['data'][0]['utc_offset'])
                    timestamp = pytz.utc.localize(timestamp)
                    timestamp = date_util.localize(timestamp + timeDelta)
                elif self.api_version == 7:
                    print("WARNING: 'timestamp_adj' processing not provided for API v7!")
                    # TODO: Figure out the date parsing needed for this.
                elif self.api_version == 4:
                    timestamp = datetime.datetime.strptime(self.collection_date.split()[0] + " 000000", "%Y-%m-%d %H%M%S")
                    timestamp = pytz.utc.localize(timestamp)
                    timestamp = date_util.localize(timestamp + timeDelta)
                if timestamp:
                    if timestamp < collDatetime:
                        json_data['header']['day_covered'] = -1
                    elif timestamp == collDatetime:
                        json_data['header']['day_covered'] = 0
                    else:
                        json_data['header']['day_covered'] = 1
                
                # Add in "timestamp_adj" for each data item:
                for item in json_data['data']:
                    if self.api_version == 8:
                        # TODO: The UTC Offset doesn't seem to reflect DST. Should we ignore it and blindly localize instead?
                        #       We can figure this out by seeing what the latest count is on a live download of the current day.
                        timestamp = datetime.datetime.strptime(self.collection_date.split()[0] + " " \
                            + ("%06d" % int(float(item['timestamp']))) + "." + str(round((item['timestamp'] % 1) * 10) * 100000),
                            "%Y-%m-%d %H%M%S.%f")
                        timestamp -= datetime.timedelta(minutes=item['utc_offset'])
                        timestamp = pytz.utc.localize(timestamp)
                        item['timestamp_adj'] = str(date_util.localize(timestamp + timeDelta))
                    elif self.api_version == 7:
                        print("WARNING: 'timestamp_adj' processing not provided for API v7!")
                        # TODO: Figure out the date parsing needed for this.
                    elif self.api_version == 4:
                        timestamp = datetime.datetime.strptime(item['timestamp'], "%Y%m%dT%H%M%S" + (".%f" if "." in item['timestamp'] else ""))
                        timestamp = pytz.utc.localize(timestamp)
                        item['timestamp_adj'] = str(date_util.localize(timestamp + timeDelta))
                        
                        item['count_version'] = int(item['count_version'])
            except KeyError:
                print("WARNING: Time representation processing has malfunctioned. Correct time key may not be present in site file.")
            except ValueError as exc:
                print("WARNING: Time representation processing has malfunctioned. Value parsing error:")
                print(exc)
            
            bjson_data = dumps(json_data).encode()

            ##write to s3 raw json bucket
            s3Object = _S3.Object(TGT_BUCKET, target_path)
            s3Object.put(Body=bjson_data)

            i += 1
            print(("JSON standardization saved as {}").format(target_path))
            print(("File {} out of {} done!").format(i, n))
            
            catCache.append((self.idBase, guid, target_path))

        # TODO: We're putting everything into the catalog at once to guard against midway failure.
        # TODO: Consider whether we want one catalog entry for all of the GUID files.
        for item in catCache:
            self.to_catalog(item[0], item[1] + ".json", item[2])

        ##put header into catalog!!!!

    def to_catalog(self, idBase, idExt, target_path):

        catalog = self.catalog
        collection_date = self.collection_date
        processing_date = self.processing_date
        json_blob = self.json_header_template

        metadata = {"repository": 'rawjson', "data_source": 'gs',
                    "id_base": idBase, "id_ext": idExt, "pointer": target_path,
                    "collection_date": collection_date,
                    "processing_date": processing_date, "metadata": json_blob}

        catalog.upsert(metadata)

"Used to keep relay() from performing needless tasks"
_relayCache = set()

def relay(catalog, idBase, idExt, collectDate, returnFile=False):
    "Moves the given file from SRC_BUCKET to TGT_BUCKET."
    
    # Get applicable file pointer from the catalog.
    # TODO: Create library utility function for this.
    command = {"select": "collection_date,pointer,id_base,id_ext,metadata",
               "repository": "eq.%s" % "raw",
               "data_source": "eq.%s" % "gs",
               "id_base": "eq.%s" % idBase,
               "id_ext": "eq.%s" % idExt,
               "collection_date": "gte.%s" % str(collectDate),
               "order": "collection_date",
               "limit": 1}
    catResults = catalog.select(params=command)
    if not catResults:
        # No record found.
        # TODO: We could look for the most recent data file up to the date.
        raise Exception("No applicable raw repo found for file base: %s, ext: %s; Date: %s" %
                        (idBase, idExt, str(collectDate)))
    lastDate = date_util.localize(arrow.get(catResults[0]["collection_date"]).datetime)
    dataPointer = catResults[0]["pointer"]
    
    fullPathR = os.path.join(_TEMP_DIR, catResults[0]["id_base"] + "_" + catResults[0]["id_ext"])
    if (idBase, idExt, lastDate) not in _relayCache:
        # Read the file:
        # TODO: This can be done in-memory:
        _S3.Bucket(SRC_BUCKET).download_file(dataPointer, fullPathR)

        # Write the file:
        with open(fullPathR, 'rb') as json_file:
            s3Object = _S3.Object(TGT_BUCKET, dataPointer)
            s3Object.put(Body=json_file)

        # Clean up:
        if not returnFile:
            os.remove(fullPathR)
        
        # Update the catalog:
        processing_date = str(date_util.localize(arrow.now().datetime))
        metadata = {"repository": 'rawjson', "data_source": 'gs',
                    "id_base": catResults[0]["id_base"], "id_ext": catResults[0]["id_ext"], "pointer": dataPointer,
                    "collection_date": str(lastDate),
                    "processing_date": processing_date, "metadata": catResults[0]["metadata"]}
        catalog.upsert(metadata)
    
        _relayCache.add((idBase, idExt, lastDate))
    return fullPathR

def main():
    "Main entry-point that takes --last_run_date parameter"
    
    global _TEMP_DIR
    global _S3
    
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
    print("gs_json_standard: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            monthsOld = int(args.months_old)
        except ValueError:
            monthsOld = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        monthsOld = DATE_EARLIEST
        
    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    _S3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    _TEMP_DIR = tempfile.mkdtemp()
    print("Created holding place: %s" % _TEMP_DIR)
    
    # Relay the unit data that's already in the "raw" bucket (don't pull from Knack again)
    today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
    ourDay = today if args.same_day else today - datetime.timedelta(days=1) # TODO: What if it is a new day and we haven't written the "raw" bucket? We need to get up to the valid last day.
    relay(catalog, config.UNIT_LOCATION, "unit_data.json", ourDay)
    
    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    uniqueDates = set()
    lastUpdateWorker = last_upd_cat.LastUpdateCat("raw", "rawjson", "gs", monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        if record.identifier[1] != "zip": # TODO: Can this be streamlined into the LastUpdateCat class?
            continue
        print("%s: %s -> %s%s" % (record.s3Path, SRC_BUCKET, TGT_BUCKET, "" if not record.missingFlag else " (missing)"))
        
        siteFilePath = relay(catalog, record.identifier[0], "site.json", record.fileDate, returnFile=True)
        # TODO: At this point, we should make the time information in the site file uniform. Somehow add ability for this to
        # be done with relay. 
        
        # Obtain site info:
        with open(siteFilePath, 'r') as siteFileHandle:
            siteFile = json.load(siteFileHandle)

        # Deal with the ZIP file:
        worker = GS_JSON_Standard(record.identifier[0], record.s3Path, record.fileDate, siteFile, catalog)
        if worker.jsonize():
            uniqueDates.add(record.fileDate)
                
    # Clean up the temporary output directory:
    shutil.rmtree(_TEMP_DIR)

    count = len(uniqueDates)
    print("Dates processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
