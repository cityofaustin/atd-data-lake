'''
bt_extract_unm_csv.py writes unmatched Bluetooth records to CSV from the Data Lake "ready" stage.

@author: Kenneth Perrine
'''

import json
import datetime
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import re
import sys

from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import config
from util import date_util

PROGRAM_DESC = """bt_extract_unm_csv.py writes unmatched Bluetooth records to CSV from the Data Lake "ready" stage."""

"S3 bucket as source"
SRC_BUCKET = config.composeBucket("ready")

def dumpUnm(startTime, endTime, devFilter=".*", repo="ready", dataSource="bt", prefix="Austin"):
    """
    Performs the extraction and dumping
    """
    # TODO: A lot of functionalities here (e.g. catalog, BT JSON) can be abstracted out into utility classes.
    
    # Get the catalog and AWS connection:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    s3 = config.getAWSSession().resource('s3')
    
    # Figure out the dates in preparation for querying the catalog:
    # TODO: Add utilities to date_util that do these things.
    startDate = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
    endDate = endTime.replace(hour=0, minute=0, second=0, microsecond=0)
    if endDate != endTime:
        # TODO: Revisit this logic.
        endDate += datetime.timedelta(days=1)
    
    # First, query the catalog to see what we've got:
    command = {"select": "collection_date,pointer",
               "repository": "eq.%s" % repo,
               "data_source": "eq.%s" % dataSource,
               "id_base": "eq.%s" % prefix,
               "id_ext": "eq.unmatched.json",
               "collection_date": ["gte.%s" % arrow.get(startDate).format(), "lt.%s" % arrow.get(endDate).format()],
               "order": "collection_date"}
    catResults = catalog.select(params=command)
    
    # Output CSV header:
    print("time,location,lat,lon,device")
    
    for result in catResults:
        # For each entry returned from the catalog, grab the file and convert to in-memory dict:
        print("Processing for %s..." % str(result["collection_date"]), file=sys.stderr)
        contentObject = s3.Object(SRC_BUCKET, result["pointer"])
        fileContent = contentObject.get()['Body'].read().decode('utf-8')
        ourData = json.loads(fileContent)

        # Prepare our supporting intersection/location data, applying filter:
        devices = {}
        regexp = re.compile(devFilter)
        for device in ourData["devices"]:
            if regexp.search(device["device_name"]):
                devices[device["device_id"]] = device

        # Third, dump out rows of CSV output that occur within the time range:
        for row in ourData["data"]:
            if row["device_id"] in devices:
                try:
                    # ourDate = date_util.localize(arrow.get(row["host_timestamp"], "M/D/YYYY H:m:s A"))
                    ourDate = date_util.localize(arrow.get(row["host_timestamp"]))
                except:
                    # There's occasional "nan" entries.
                    # TODO: Figure out where "nan" entries come from.
                    # TODO: Shall we break on malformatted dates?
                    continue
                if ourDate >= startTime and ourDate < endTime:
                    # Only do this if the filter was satisfied:
                    ourDevice = devices[row["device_id"]]
                    print("%s,%s,%.4f,%.4f,%s" % (str(ourDate), ourDevice["device_name"],
                                                  float(ourDevice["lat"]), float(ourDevice["lon"]), row["dev_addr"]))

def main():
    """
    Entry point and command line parser.
    """
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-s", "--starttime", help="start time (format: YYYY-MM-DD HH:MM), or yesterday if not specified")
    parser.add_argument("-e", "--endtime", help="end time, exclusive (format: YYYY-MM-DD HH:MM), or starttime + 1 day if not specified")
    parser.add_argument("-f", "--devname_filter", default=".*", help="filter results on units whose device names match the given regexp")
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    if args.starttime:
        startTime = date_util.parseDate(args.starttime, dateOnly=False)
    else:
        startTime = date_util.localize(datetime.datetime.now())
        startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if args.endtime:
        endTime = date_util.parseDate(args.endtime, dateOnly=False)
    else:
        endTime = startTime + datetime.timedelta(days=1)
        
    dumpUnm(startTime, endTime, args.devname_filter)

if __name__ == "__main__":
    sys.exit(main())
