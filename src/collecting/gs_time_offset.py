'''
Shows time offsets for each GRIDSMART device, drawing from "raw" bucket.
'''
# This uses a bunch of copied code from "gs_json_standard.py" and was rapidly put together. Don't use this for
# inspiration on other codes until we're able to clean it up.

import os
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
from aws_transport.support import gs_investigate, config
from util import date_util

PROGRAM_DESC = "Shows time offsets for each GRIDSMART device, drawing from 'raw' bucket."

"S3 bucket as source"
SRC_BUCKET = "atd-data-lake-raw"

"Temporary directory holding-place"
_TEMP_DIR = None

"S3 object"
_S3 = None

class GS_JSON_Standard:
    '''Class standardizes GRIDMSMART directory data into json,
    maintains file per guid'''

    def __init__(self, identifier, storagePath, collection_date, siteFile, catalog, ret):

        ##csv paths should be a directory with guid as key and corresponding
        ##csv file path as value

        self.identifier = identifier
        self.storagePath = storagePath
        
        self.collection_date = str(collection_date)
        self.processing_date = str(date_util.localize(arrow.now().datetime))
        self.api_version = None
        self.columns = None
        self.json_header_template = self.set_json_header_template()
        self.siteFile = siteFile
        self.catalog = catalog
        self.ret = ret
        
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
                                "zip_name": self.identifier + ".zip",
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
        fullPathR = os.path.join(_TEMP_DIR, self.identifier + ".zip")
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

        self.api_version = self.get_api_version(file_dict)
        self.set_data_columns()
        for key, value in file_dict.items():

            guid = key
            csv_path = value #recall this is a temporary location from unzipped
            target_filename = self.identifier + '_' + guid + '.json'
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
                #collDatetime = arrow.get(self.collection_date).datetime
                #collDatetime = collDatetime.replace(hour=0, minute=0, second=0, microsecond=0)
                timestamp = None
                timestamp0 = None
                if self.api_version == 8 and json_data['data']:
                    timestamp = datetime.datetime.strptime(self.collection_date.split()[0] + " 000000", "%Y-%m-%d %H%M%S")
                    timestamp0 = date_util.localize(timestamp)
                    timestamp -= datetime.timedelta(minutes=json_data['data'][0]['utc_offset'])
                    timestamp = pytz.utc.localize(timestamp)
                    timestamp = date_util.localize(timestamp + timeDelta)
                elif self.api_version == 7:
                    print("WARNING: 'timestamp_adj' processing not provided for API v7!")
                    # TODO: Figure out the date parsing needed for this.
                elif self.api_version == 4:
                    timestamp = datetime.datetime.strptime(self.collection_date.split()[0] + " 000000", "%Y-%m-%d %H%M%S")
                    timestamp0 = date_util.localize(timestamp)
                    timestamp = pytz.utc.localize(timestamp)
                    timestamp = date_util.localize(timestamp + timeDelta)
                        
            except KeyError:
                print("WARNING: Time representation processing has malfunctioned. Correct time key may not be present in site file.")
                
            break

        rec = {"street1": self.siteFile["site"]["Location"]["Street1"],
               "street2": self.siteFile["site"]["Location"]["Street2"],
               "version": self.api_version,
               #"offset_sec": (collDatetime - timestamp0).total_seconds(),
               "offset_adj_sec": (timestamp - timestamp0).total_seconds()}
        self.ret.append(rec)

    def to_catalog(self, identifier, target_path):

        catalog = self.catalog
        #identifier = self.identifier
        #pointer = self.target_filepath
        collection_date = self.collection_date
        processing_date = self.processing_date
        json_blob = self.json_header_template

        metadata = {"repository": 'rawjson', "data_source": 'gs',
                    "identifier": identifier, "pointer": target_path,
                    "collection_date": collection_date,
                    "processing_date": processing_date, "metadata": json_blob}

        catalog.upsert(metadata)

def readFile(catalog, fileBase, collectDate):
    "Reads file from SRC_BUCKET."
    
    # Get applicable file pointer from the catalog.
    # TODO: Create library utility function for this.
    command = {"select": "collection_date,pointer,identifier,metadata",
               "repository": "eq.%s" % "raw",
               "data_source": "eq.%s" % "gs",
               "identifier": "like.%s*" % fileBase, # TODO: Use exact query when we don't use date.
               "collection_date": "gte.%s" % str(collectDate),
               "order": "collection_date",
               "limit": 1}
    catResults = catalog.select(params=command)
    if not catResults:
        # No record found.
        # TODO: We could look for the most recent data file up to the date.
        raise Exception("No applicable raw repo found for file base: %s; Date: %s" %
                        (fileBase, str(collectDate)))
    dataPointer = catResults[0]["pointer"]
    
    fullPathR = os.path.join(_TEMP_DIR, catResults[0]["identifier"] + ".json")
    _S3.Bucket(SRC_BUCKET).download_file(dataPointer, fullPathR)

    return fullPathR

def main():
    "Main entry-point that takes --last_run_date parameter"
    
    global _TEMP_DIR
    global _S3
    
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-d", "--date", help="sample collection date in YYYY-MM-DD format")
    parser.add_argument("-o", "--output", help="output filename for the CSV contents")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    ourDate = date_util.parseDate(args.date, dateOnly=True)
        
    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    _S3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    _TEMP_DIR = tempfile.mkdtemp()
    print("Created holding place: %s" % _TEMP_DIR)
    
    # Gather records of prior activity from catalog:
    command = {"select": "collection_date,identifier,pointer",
               "repository": "eq.%s" % 'raw',
               "data_source": "eq.%s" % 'gs',
               "collection_date": ["gte.%s" % arrow.get(ourDate).format(), "lt.%s" % arrow.get(ourDate + datetime.timedelta(1)).format()],
               "order": "collection_date"}
    catResults = catalog.select(params=command)
    
    ret = []
    for result in catResults:
        if not result["pointer"].lower().endswith(".zip"):
            # TODO: When we get our ext identifiers, take this hack out.
            continue
        print("-> %s" % result["pointer"])
        fileDate = arrow.get(result["collection_date"]).datetime

        datePart = fileDate.strftime("%Y-%m-%d") + "_"
        basename = result["identifier"][len(datePart):]
        siteFilePath = readFile(catalog, basename + "_site", fileDate)
        
        # Obtain site info:
        with open(siteFilePath, 'r') as siteFileHandle:
            siteFile = json.load(siteFileHandle)

        # Deal with the ZIP file:
        worker = GS_JSON_Standard(result["identifier"], result["pointer"], fileDate, siteFile, catalog, ret)
        worker.jsonize()
    
    # Build up DataFrame and output CSV.
    print("Writing output...")
    df = pd.DataFrame(ret)
    df.to_csv(args.output, index=False)
                
    # Clean up the temporary output directory:
    shutil.rmtree(_TEMP_DIR)
    print("Done.")

if __name__ == "__main__":
    main()
