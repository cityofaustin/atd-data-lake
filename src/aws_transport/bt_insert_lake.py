'''
Rough script of bt metadata ingestion into postgrest for CoA Data project
 + movement of bt files to s3
@ Nadia Florez
'''
import json
import os
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import last_upd_fs, config
from util import date_util, date_dirs

PROGRAM_DESC = "Inserts Bluetooth data from AWAM share into the Raw Data Lake"

"Number of months to go back for filing records"
DATE_EARLIEST = 12
# TODO: Consider months instead of years.

"S3 bucket to target"
BUCKET = "atd-data-lake-raw"

DIR_DEFS = [date_dirs.DateDirDef(prefix="Austin_bt_", dateFormat="%m-%d-%Y", postfix=".txt"),
            date_dirs.DateDirDef(prefix="Austin_btmatch_", dateFormat="%m-%d-%Y", postfix=".txt"),
            date_dirs.DateDirDef(prefix="Austin_bt_summary_15_", dateFormat="%m-%d-%Y", postfix=".txt")]

## Function definitions
def set_S3_pointer(filename, date, data_source='bt'): ### may have to include bucket!! ###

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
def bt_metadata(repository, file, pointer, collectionDate):

    processing_date = str(date_util.localize(arrow.now().datetime))
    json_blob = json.dumps({"element": "True"})

    metadata = {"repository": repository, "data_source": 'bt',
               "identifier": file, "pointer": pointer,
               "collection_date": str(collectionDate),
               "processing_date": processing_date, "metadata": json_blob}

    return metadata

def bt_metadata_ingest(metadata, catalog):

    catalog.upsert(metadata)

def main():
    "Main entry-point that takes --last_run_date parameter"
    
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-d", "--source_dir", default=".", help="Source directory (e.g. AWAM share) to read Bluetooth files from")
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time and time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-M", "--missing", action="store_true", default=False, help="check for missing entries after the earliest processing date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=False)
    print("bt_insert_lake: Last run date: %s" % str(lastRunDate))

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

    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    lastUpdateWorker = last_upd_fs.LastUpdateFS(args.source_dir, "raw", "bt", DIR_DEFS, monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        filename = os.path.basename(record.filePath)
        baseFile = filename[:-4] if filename.lower().endswith(".txt") else filename

        pointer = set_S3_pointer(filename=filename, date=record.fileDate)
        
        print("%s -> %s:%s%s" % (filename, BUCKET, pointer, "" if not record.missingFlag else " (missing)"))

        # Put TXT to S3:
        with open(record.filePath, 'rb') as wt_file:
            s3Object = s3.Object(BUCKET, pointer)
            s3Object.put(Body=wt_file)
        
        # Update the catalog:
        bt_metadata_ingest(bt_metadata(repository='raw', file=baseFile,
                                      pointer=pointer, collectionDate=record.fileDate), catalog=catalog)

        # Increment count:
        count += 1
        
    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
