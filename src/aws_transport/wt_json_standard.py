'''
json standardization for wavetronix
Author: Nadia Florez
'''
import os
from json import dumps
import tempfile
import shutil
from argparse import ArgumentParser, RawDescriptionHelpFormatter

import pandas as pd
from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import last_upd_cat, config
from util import date_util

PROGRAM_DESC = "Performs JSON canonicalization for Wavetronix data between the raw and rawjson Data Lake buckets"

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

class WT_JSON_Standard:
    ''' 'Class standardizes wavetronix directory data into json'''

    def __init__(self, identifier, storagePath, collection_date, catalog):

        self.identifier = identifier
        self.srcStoragePath = storagePath
        self.tgtStoragePath = storagePath[:-4] + ".json" # TODO: Maybe let storagePath just be the S3 path: use identifier.
        # TODO: Provide standardized method to reconstruct the S3 path.
        self.collection_date = str(collection_date)
        self.processing_date = str(date_util.localize(arrow.now().datetime))
        self.columns = self._set_data_columns()
        self.json_header_template = self._set_json_header_template()
        self.catalog = catalog

    @staticmethod
    def _set_data_columns():

        wt_data_columns = ["curdatetime", "day", "day_of_week",
                           "detid", "direction", 'hour', 'int_id',
                           'intname', 'minute', 'month', 'occupancy',
                           'row_id', 'speed', 'timebin', 'volume', 'year']
        return wt_data_columns

    def _set_json_header_template(self):

        json_header_template = {"data_source": "wavetronix",
                                "origin_filename": self.identifier + ".csv",
                                "target_filename": self.identifier + ".json",
                                "collection_date": self.collection_date,
                                "processing_date": self.processing_date}
        return json_header_template

    def jsonize(self):

        #initiate json object
        json_data = {'header': self.json_header_template, 'data': None}
        #call bucket
        fullPathR = os.path.join(_TEMP_DIR, self.identifier + ".csv")
        _S3.Bucket(SRC_BUCKET).download_file(self.srcStoragePath, fullPathR)
        # add data array of objects from rows of dataframe
        data = pd.read_csv(fullPathR, header=0, names=self.columns)
        json_data['data'] = data.apply(lambda x: x.to_dict(), axis=1).tolist()

        ##write to s3 raw json bucket
        fullPathW = os.path.join(_TEMP_DIR, self.identifier + ".json")
        with open(fullPathW, 'w') as wt_json_file:
            wt_json_file.write(dumps(json_data))

        with open(fullPathW, 'rb') as wt_json_file:
            s3Object = _S3.Object(TGT_BUCKET, self.tgtStoragePath)
            s3Object.put(Body=wt_json_file)
            
        # Clean up:
        os.remove(fullPathR)
        os.remove(fullPathW)

    def to_catalog(self):

        catalog = self.catalog
        identifier = self.identifier
        pointer = self.tgtStoragePath
        collection_date = self.collection_date
        processing_date = self.processing_date
        json_blob = self.json_header_template

        metadata = {"repository": 'rawjson', "data_source": 'wt',
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
    parser.add_argument("-M", "--missing", action="store_true", default=False, help="check for missing entries after the earliest processing date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("wt_json_standard: Last run date: %s" % str(lastRunDate))

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
    
    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    lastUpdateWorker = last_upd_cat.LastUpdateCat("raw", "rawjson", "wt", monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        print("%s: %s -> %s%s" % (record.s3Path, SRC_BUCKET, TGT_BUCKET, "" if not record.missingFlag else " (missing)"))
        worker = WT_JSON_Standard(record.identifier, record.s3Path, record.fileDate, catalog)
        worker.jsonize()
        worker.to_catalog()
        
        # Increment count:
        count += 1
    
    # Clean up the temporary output directory:
    shutil.rmtree(_TEMP_DIR)

    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
