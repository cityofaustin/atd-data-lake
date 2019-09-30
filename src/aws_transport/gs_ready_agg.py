'''
CoA Data Lake 'ready' bucket for GRIDSMART, with aggregation
Author: Kenneth Perrine, Nadia Florez
'''

import json
import os
import numbers
import tempfile
import shutil
from argparse import ArgumentParser, RawDescriptionHelpFormatter

import pandas as pd
import numpy as np
from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import config
from util import date_util

PROGRAM_DESC = "Aggregates 'ready' Data Lake bucket GRIDSMART counts"

"Number of months to go back for filing records"
DATE_EARLIEST = 12

"S3 bucket as source"
SRC_BUCKET = "atd-data-lake-ready"
    
def set_S3_pointer(filename, date, data_source='gs'):

    year = str(date.year)
    month = str(date.month)
    day = str(date.day)

    s_year = year
    s_month = month if len(month) == 2 else month.zfill(2)
    s_day = day if len(day) == 2 else day.zfill(2)

    return "{year}/{month}/{day}/{data_source}/{file}".format(year=s_year,
                                                            month=s_month,
                                                            day=s_day,
                                                            data_source=data_source,
                                                            file=filename)

def main():
    "Main entry-point that takes --last_run_date parameter"

    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-e", "--end_date", help="end date, in YYYY-MM-DD (default: today)")
    parser.add_argument("-M", "--missing", action="store_true", default=False, help="check for missing entries after the earliest processing date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    parser.add_argument("-a", "--agg", type=int, default=15, help="aggregation interval, in minutes (default: 15)")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("gs_ready_agg: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            dateEarliest = int(args.months_old)
        except ValueError:
            dateEarliest = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        dateEarliest = DATE_EARLIEST

    if args.end_date:
        endDate = date_util.parseDate(args.end_date, dateOnly=True)
    else:
        endDate = None

    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    s3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    tempDir = tempfile.mkdtemp()
    print("Created holding place: %s" % tempDir)

    # TODO: Once the base/ext identification scheme is in place, change over to LastUpdateCat to select which files to process.
    # Figure out the dates in preparation for querying the catalog:
    # TODO: Add utilities to date_util that do these things.
    if isinstance(dateEarliest, numbers.Number):
        dateEarliest = date_util.localize(arrow.now()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .shift(months=-dateEarliest).datetime)
    if lastRunDate > dateEarliest:
        dateEarliest = lastRunDate
    today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)

    startDate = dateEarliest.replace(hour=0, minute=0, second=0, microsecond=0)
    if not endDate:
        endDate = today.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # First, query the catalog to see what we've got:
    command = {"select": "collection_date,identifier,pointer",
               "repository": "eq.%s" % "ready",
               "data_source": "eq.%s" % "gs",
               "identifier": "like.*%s*" % "_counts_", # TODO: Use the base/ext scheme
               "collection_date": ["gte.%s" % arrow.get(startDate).format(), "lte.%s" % arrow.get(endDate).format()],
               "limit": 1000000,
               "order": "collection_date"}
    catResults = catalog.select(params=command)

    count = 0    
    for result in catResults:
        print("Processing: %s" % result["identifier"])
        
        # For each entry returned from the catalog, grab the file:
        fullPathR = os.path.join(tempDir, result["identifier"] + ".json")
        s3.Bucket(SRC_BUCKET).download_file(result["pointer"], fullPathR)
        with open(fullPathR, 'r') as dataJSONFile:
            dataJSON = json.load(dataJSONFile)
        os.remove(fullPathR)

        header = dataJSON["header"]
        
        # Collect movement information:
        movements = []
        for camera in dataJSON["site"]["site"]["CameraDevices"]:
            for zoneMask in camera["Fisheye"]["CameraMasks"]["ZoneMasks"]:
                if "Vehicle" in zoneMask:
                    movements.append({"zone_approach": zoneMask["Vehicle"]["ApproachType"],
                                      "turn_type": zoneMask["Vehicle"]["TurnType"],
                                      "zone": zoneMask["Vehicle"]["Id"]})
        
        # Process the counts:
        data = pd.DataFrame(dataJSON["counts"])
        data['heavy_vehicle'] = np.where(data.vehicle_length < 17, 0, 1)
        # In the following line, we convert to UTC because there's a bug in the grouper that doesn't deal with
        # the end of daylight savings time.
        data['timestamp'] = pd.to_datetime(data["timestamp_adj"], utc=True)
        data = data.merge(pd.DataFrame(movements), on='zone')

        # Do the grouping:        
        colValues = [pd.Grouper(key='timestamp', freq=('%ds' % (args.agg * 60))), 'zone_approach', 'turn', 'heavy_vehicle']
        grouped = data.groupby(colValues)
        volume = grouped.size().reset_index(name='volume')
        avgSpeed = grouped.agg({'speed': 'mean'}).round(3).reset_index().rename(columns={'speed': 'speed_avg'})
        stdSpeed = grouped.agg({'speed': 'std'}).fillna(0).round(3).reset_index().rename(columns={'speed': 'speed_std'})
        avgSecInZone = grouped.agg({'seconds_in_zone': 'mean'}).round(3).reset_index().rename(columns={'seconds_in_zone': 'seconds_in_zone_avg'})
        stdSecInZone = grouped.agg({'seconds_in_zone': 'std'}).round(3).fillna(0).reset_index().rename(columns={'seconds_in_zone': 'seconds_in_zone_std'})

        # Merging all information
        colValues[0] = "timestamp"
        summarized = volume.merge(avgSpeed, on=colValues).merge(stdSpeed, on=colValues).merge(avgSecInZone, on=colValues).merge(stdSecInZone, on=colValues)
        summarized = summarized[['timestamp', 'zone_approach', 'turn', 'heavy_vehicle',
                                'volume', 'speed_avg', 'speed_std', 'seconds_in_zone_avg', 'seconds_in_zone_std']]
        # While converting the timestamp to a string, we also convert it back to our local time zone to counter
        # the grouping/UTC workaround that was performed above.
        summarized["timestamp"] = summarized["timestamp"].dt.tz_convert(date_util.LOCAL_TIMEZONE).astype(str)
        
        # Update the header
        header["processing_date"] = str(date_util.localize(arrow.now().datetime))
        header["agg_interval_sec"] = args.agg * 60
        
        # Assemble together the aggregation file:
        newFileContents = {"header": header,
                           "data": summarized.apply(lambda x: x.to_dict(), axis=1).tolist(),
                           "site": dataJSON["site"],
                           "device": dataJSON["device"]}
        
        # Write aggregation to S3, write to the catalog:
        base = result["identifier"].split("_counts_")[0]
        ourDate = date_util.localize(arrow.get(result["collection_date"]).datetime)
        targetBaseFile = base + ("_agg%d_" % args.agg) + ourDate.strftime("%Y-%m-%d")
        targetPath = set_S3_pointer(targetBaseFile + ".json", ourDate)
                
        print("%s: %s" % (SRC_BUCKET, targetPath))
        fullPathW = os.path.join(tempDir, targetBaseFile + ".json")
        with open(fullPathW, 'w') as gsJSONFile:
            json.dump(newFileContents, gsJSONFile)

        with open(fullPathW, 'rb') as gsJSONFile:
            s3Object = s3.Object(SRC_BUCKET, targetPath)
            s3Object.put(Body=gsJSONFile)
            
        # Clean up:
        os.remove(fullPathW)

        # Update the catalog:
        metadata = {"repository": 'ready', "data_source": 'gs',
                    "identifier": targetBaseFile, "pointer": targetPath,
                    "collection_date": header["collection_date"],
                    "processing_date": header["processing_date"],
                    "metadata": {"artifact_type": "aggregation",
                                 "agg_interval_sec": header["agg_interval_sec"]}}
        catalog.upsert(metadata)
        
        # Increment count:
        count += 1

    # Clean up the temporary output directory:
    shutil.rmtree(tempDir)

    print("Records processed: %d" % count)
    return count

if __name__ == "__main__":
    main()
