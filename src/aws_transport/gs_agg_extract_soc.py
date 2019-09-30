'''
Extract GRIDSMART aggregate "ready" data to Socrata
Author: Kenneth Perrine
'''
import json
import hashlib
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from tdutils import socratautil
import arrow

import _setpath
from aws_transport.support import last_upd_soc, config
from util import date_util

PROGRAM_DESC = "Extracts GRIDSMART aggregate 'ready' data to Socrata"

"Number of months to go back for filing records"
DATE_EARLIEST = 12

"S3 bucket as source"
SRC_BUCKET = "atd-data-lake-ready"

"S3 object"
_S3 = None

SOC_CHUNK = 10000
"SOC_CHUNK is the number of entries per transaction."

def socTime(inTimeStr):
    "Converts the canonicalized time to the time representation that Socrata uses."
    return arrow.get(inTimeStr).datetime.strftime("%Y-%m-%dT%H:%M:%S")

def _addErrDup(errDup, errStr):
    if errStr not in errDup: 
        errDup[errStr] = 0
    errDup[errStr] += 1

def main():
    "Main entry-point that takes --last_run_date parameter"
    
    global _S3
    
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-e", "--end_date", help="end date, in YYYY-MM-DD (default: today)")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    parser.add_argument("-a", "--agg", type=int, default=15, help="aggregation interval, in minutes (default: 15)")
    parser.add_argument("-u", "--no_unassigned", action="store_true", default=False, help="skip 'unassigned' approaches")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("gs_agg_extract_soc: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            monthsOld = int(args.months_old)
        except ValueError:
            monthsOld = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        monthsOld = DATE_EARLIEST

    if args.end_date:
        endDate = date_util.parseDate(args.end_date, dateOnly=True)
    else:
        endDate = None

    # AWS connection:
    _S3 = config.getAWSSession().resource('s3')
    
    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    # TODO: Rather than seeing if a record exists in Socrata, it would be better to have an inventory.
    lastUpdateWorker = last_upd_soc.LastUpdateSoc("ready", config.SOC_RESOURCE_GS_AGG, "host_read_time", config.SOC_WRITE_AUTH, "gs", monthsOld, endDate=endDate)
    for record in lastUpdateWorker.iterToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=False):
        # TODO: We need to distinguish different file types. Use the base/ext thing later on to do this better.
        if ("_agg%d_" % args.agg) not in record.identifier:
            continue
        
        print("%s: %s -> %s%s" % (record.s3Path, SRC_BUCKET, config.SOC_RESOURCE_GS_AGG, "" if not record.missingFlag else " (missing)"))
        
        # Pull from S3
        contentObject = _S3.Object(SRC_BUCKET, record.s3Path)
        fileContent = contentObject.get()['Body'].read().decode('utf-8')
        ourData = json.loads(fileContent)
        device = ourData["device"] if "device" in ourData else None
        
        # Contingency for bad device info:
        if not device:
            device = {"atd_device_id": None,
                      "primary_st": ourData["site"]["site"]["Location"]["Street1"],
                      "cross_st": ourData["site"]["site"]["Location"]["Street2"]}
            print("WARNING: Device for %s / %s has no device information. Skipping." % (device["primary_st"], device["cross_st"]))
            continue # Comment this out if we're to record the site information after all.

        # Assemble JSON for Socrata
        hasher = hashlib.md5()
        dataRows = []
        rowCount = 0
        chunkCount = SOC_CHUNK
        errDup = {}
        for line in ourData["data"]:
            if chunkCount == SOC_CHUNK:
                print("Rows %d-%d..." % (rowCount + 1, min(rowCount + SOC_CHUNK, len(ourData["data"]))))
            
            approach = line["zone_approach"]
            if approach == "Southbound":
                approach = "SOUTHBOUND"
            elif approach == "Northbound":
                approach = "NORTHBOUND"
            elif approach == "Eastbound":
                approach = "EASTBOUND"
            elif approach == "Westbound":
                approach = "WESTBOUND"
            elif approach == "Unassigned" and not args.no_unassigned:
                approach = "UNASSIGNED"
                _addErrDup(errDup, "WARNING: Approach is UNASSIGNED. Including.")
            else:
                _addErrDup(errDup, "WARNING: Approach is %s. Skipping." % approach)
                continue
                
            movement = line["turn"]
            if movement == "S":
                movement = "THRU"
            elif movement == "L":
                movement = "LEFT TURN"
            elif movement == "R":
                movement = "RIGHT TURN"
            elif movement == "U":
                movement = "U-TURN"
            else:
                _addErrDup(errDup, "WARNING: Movement is %s" % movement)
            
            timestamp = arrow.get(line["timestamp"])
                
            entry = {"atd_device_id": device["atd_device_id"],
                     "read_date": socTime(line["timestamp"]),
                     "intersection_name": device["primary_st"].strip() + " / " + device["cross_st"].strip(),
                     "direction": approach,
                     "movement": movement,
                     "heavy_vehicle": line["heavy_vehicle"] != 0,
                     "volume": line["volume"],
                     "speed_average": line["speed_avg"],
                     "speed_stddev": line["speed_std"],
                     "seconds_in_zone_average": line["seconds_in_zone_avg"],
                     "seconds_in_zone_stddev": line["seconds_in_zone_std"],
                     "month": timestamp.month,
                     "day": timestamp.day,
                     "year": timestamp.year,
                     "hour": timestamp.hour,
                     "minute": timestamp.minute,
                     "day_of_week": (timestamp.weekday() + 1) % 7,
                     "bin_duration": args.agg * 60
            }
            hashFields = ["intersection_name", "read_date", "heavy_vehicle", "direction", "movement"]

            hashStr = "".join([str(entry[q]) for q in hashFields])
            hasher.update(hashStr.encode("utf-8"))
            entry["record_id"] = hasher.hexdigest()
                    
            dataRows.append(entry)
            
            chunkCount -= 1
            rowCount += 1
            if chunkCount == 0 or rowCount == len(ourData["data"]): 
                # Push to Socrata
                s = socratautil.Soda(auth=config.SOC_WRITE_AUTH, records=dataRows, resource=config.SOC_RESOURCE_GS_AGG, location_field=None, source="datalake")
                
                dataRows = []
                chunkCount = SOC_CHUNK
        
        # Increment count:
        count += 1
        
        # Output warnings:
        for errMsg in errDup:
            print(errMsg + ": (x%d)" % errDup[errMsg])
        
    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
