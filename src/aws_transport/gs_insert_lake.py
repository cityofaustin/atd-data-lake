'''
Rough script of gs metadata ingestion into postgrest for CoA Data project
 + movement of gs files to s3
@ Nadia Florez

Note: not sure if this is necessary as the zip files can be placed in s3 bucke when called!
'''
import json
import os
import tempfile
import shutil
import datetime
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import last_upd_gs, config
from util import date_util

PROGRAM_DESC = "Inserts GRIDSMART data from field devices into the Raw Data Lake"

"Number of months to go back for filing records"
DATE_EARLIEST = 12
# TODO: Consider months instead of years.

"S3 bucket to target"
BUCKET = config.composeBucket("raw")

"Temporary directory holding-place"
_TEMP_DIR = None

"S3 object"
_S3 = None

## Function definitions
def set_S3_pointer(filename, date, data_source='gs'): ### may have to include bucket!! ###

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
def gs_metadata(repository, idBase, idExt, pointer, date):

    collection_date = str(date)
    processing_date = str(date_util.localize(arrow.now().datetime))
    json_blob = json.dumps({"element": "True"}) #maybe add version metadata?

    metadata = {"repository": repository, "data_source": 'gs',
               "id_base": idBase, "id_ext": idExt, "pointer": pointer,
               "collection_date": collection_date,
               "processing_date": processing_date, "metadata": json_blob}

    return metadata

def gs_metadata_ingest(metadata, catalog):

    catalog.upsert(metadata)
    
def insertUnitData(areaBase, catalog, unitData, sameDay=False):
    "Handles S3 upload and catalog insert for unit data."

    # TODO: A great deal of this can be handled with shared utility functions.
    # Get "collection date":
    today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
    ourDay = today if sameDay else today - datetime.timedelta(days=1)
    
    # Get filename and target path: 
    baseName = "{}_unit_data_{}".format(areaBase, ourDay.strftime("%Y-%m-%d")) # TODO: We may phase out the use of a date in the identifier.
    tgtStorage_path = set_S3_pointer(filename=baseName + ".json",
                                         date=ourDay, data_source="gs")
    print("%s:%s" % (BUCKET, tgtStorage_path))
    
    # Arrange the JSON:
    json_header_template = {"data_type": "gs_unit_data",
                            "target_filename": baseName + ".json",
                            "collection_date": str(ourDay)}
    json_data = {'header': json_header_template,
                 'devices': unitData}
    
    # Write to S3:
    # TODO: Again, create shared utility functions. There is a way to do this in-memory, too.
    fullPathW = os.path.join(_TEMP_DIR, baseName + ".json")
    with open(fullPathW, 'w') as json_file:
        json_file.write(json.dumps(json_data))

    with open(fullPathW, 'rb') as json_file:
        s3Object = _S3.Object(BUCKET, tgtStorage_path)
        s3Object.put(Body=json_file)

    # Clean up:
    os.remove(fullPathW)

    # Update the catalog:
    # TODO: Have mercy! Use library functions!
    processing_date = str(date_util.localize(arrow.now().datetime))
    metadata = {"repository": 'raw', "data_source": 'gs',
                "id_base": areaBase, "id_ext": "unit_data.json", "pointer": tgtStorage_path,
                "collection_date": str(ourDay),
                "processing_date": processing_date, "metadata": json_header_template}
    catalog.upsert(metadata)

def insertSiteFile(catalog, baseFile, fileContents, device, sameDay=False):
    "Handles S3 upload and catalog insert for site file."

    # TODO: A great deal of this can be handled with shared utility functions.
    # Get "collection date":
    today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
    ourDay = today if sameDay else today - datetime.timedelta(days=1)
    
    # Get filename and target path: 
    siteFilename = "{}_site_{}".format(baseFile, ourDay.strftime("%Y-%m-%d"))
    tgtStorage_path = set_S3_pointer(filename=siteFilename + ".json",
                                         date=ourDay, data_source="gs")
    print("%s:%s" % (BUCKET, tgtStorage_path))
    
    # Arrange the JSON:
    json_header_template = {"data_type": "gs_site",
                            "target_filename": siteFilename + ".json",
                            "collection_date": str(ourDay),
                            "device_net_addr": device.netAddr}
    json_data = {'header': json_header_template,
                 'site': fileContents["site"],
                 'datetime': fileContents["time"],
                 'hardware_info': fileContents["hardware_info"]}
    
    # Write to S3:
    # TODO: Again, create shared utility functions. There is a way to do this in-memory, too.
    fullPathW = os.path.join(_TEMP_DIR, siteFilename + ".json")
    with open(fullPathW, 'w') as json_file:
        json_file.write(json.dumps(json_data))

    with open(fullPathW, 'rb') as json_file:
        s3Object = _S3.Object(BUCKET, tgtStorage_path)
        s3Object.put(Body=json_file)

    # Clean up:
    os.remove(fullPathW)

    # Update the catalog:
    # TODO: Leverage library functions for this.
    # TODO: Fix the identifier to the newer standard: no date, base/ext
    processing_date = str(date_util.localize(arrow.now().datetime))
    metadata = {"repository": 'raw', "data_source": 'gs',
                "id_base": baseFile, "id_ext": "site.json", "pointer": tgtStorage_path,
                "collection_date": str(ourDay),
                "processing_date": processing_date, "metadata": json_header_template}
    catalog.upsert(metadata)

def main():
    "Main entry-point that takes --last_run_date parameter"
    global _TEMP_DIR, _S3
    
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-M", "--missing", action="store_true", default=False, help="check for missing entries after the earliest processing date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    parser.add_argument("-f", "--devname_filter", default=".*", help="filter processing on units whose street names match the given regexp")
    
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("gs_insert_lake: Last run date: %s" % str(lastRunDate))

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
    
    # Construct list of all GRIDSMART devices and log readers:
    logReaders, allFiles, knackJSON = last_upd_gs.getDevicesLogreaders(devFilter=args.devname_filter)
    
    # Upload the Knack data:
    print("Uploading unit data...")
    insertUnitData(config.UNIT_LOCATION, catalog, knackJSON, args.same_day)
    
    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    lastUpdateWorker = last_upd_gs.LastUpdateGS(logReaders, "raw", "gs", monthsOld)
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        try:
            filePath = record.logReader.getCountsFile(record.fileDate.replace(tzinfo=None), _TEMP_DIR)
        except Exception as exc:
            print("ERROR: A problem was encountered in accessing.") 
            print(exc)
            continue
        baseFile = record.logReader.constructBase()
        
        pointer = set_S3_pointer(filename=baseFile + "_" + record.fileDate.strftime("%Y-%m-%d") + ".zip", date=record.fileDate)
        
        print("%s -> %s:%s%s" % (baseFile + ".zip", BUCKET, pointer, "" if not record.missingFlag else " (missing)"))

        # Put ZIP to S3:
        with open(filePath, 'rb') as wt_file:
            s3Object = _S3.Object(BUCKET, pointer)
            s3Object.put(Body=wt_file)
                
        # Update the catalog:
        gs_metadata_ingest(gs_metadata(repository='raw', idBase=record.identifier[0], idExt=record.identifier[1],
                                      pointer=pointer, date=record.fileDate), catalog=catalog)
        # Clean up ZIP file:
        os.remove(filePath)

        # Device site file upload:
        insertSiteFile(catalog, baseFile, allFiles[record.device], record.device, args.same_day)

        # Increment count:
        count += 1
    
    # Clean up the temporary output directory:
    shutil.rmtree(_TEMP_DIR)
    
    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
