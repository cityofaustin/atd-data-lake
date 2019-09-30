'''
Extract Bluetooth "Ready" Data Lake data to Socrata
Author: Nadia Florez
'''
import json
import hashlib
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from tdutils import socratautil
import arrow

import _setpath
from aws_transport.support import last_upd_soc, config
from util import date_util

PROGRAM_DESC = "Extracts Bluetooth files from the 'Ready' bucket to Socrata"

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

def main():
    "Main entry-point that takes --last_run_date parameter"
    
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
    print("bt_extract_soc: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            monthsOld = int(args.months_old)
        except ValueError:
            monthsOld = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        monthsOld = DATE_EARLIEST
    
    # AWS connection:
    _S3 = config.getAWSSession().resource('s3')

    # Gather records of prior activity from catalog:
    print("Beginning loop...")
    count = 0
    addrLookupDate = None
    addrLookup = {}
    addrLookupCounter = 0
    # TODO: Rather than seeing if a record exists in Socrata, it would be better to have an inventory.
    lastUpdateWorker = last_upd_soc.LastUpdateSoc("ready", config.SOC_RESOURCE_BT_IAF, "host_read_time", config.SOC_WRITE_AUTH, "bt", monthsOld)
    for record in lastUpdateWorker.iterToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=False):
        # TODO: We need to distinguish different file types. Use the base/ext thing later on to do this better.
        fileType = None
        socResource = None
        if "_bt_summary_15_" in record.identifier:
            fileType = "traf_match_summary"
            socResource = config.SOC_RESOURCE_BT_TMSR
        elif "_btmatch_" in record.identifier:
            fileType = "matched"
            socResource = config.SOC_RESOURCE_BT_ITMF
        elif "_bt_" in record.identifier:
            fileType = "unmatched"
            socResource = config.SOC_RESOURCE_BT_IAF
        else:
            continue 
        print("%s: %s -> %s%s" % (record.s3Path, SRC_BUCKET, socResource, "" if not record.missingFlag else " (missing)"))
        
        # These variables will keep track of the device counter that gets reset daily:
        if record.fileDate != addrLookupDate:
            addrLookupDate = record.fileDate
            addrLookup = {}
            addrLookupCounter = 0
        
        # Pull from S3
        contentObject = _S3.Object(SRC_BUCKET, record.s3Path)
        fileContent = contentObject.get()['Body'].read().decode('utf-8')
        ourData = json.loads(fileContent)
        
        # Generate device lookup:
        devices = {d["device_id"]: d for d in ourData["devices"]}
        
        # Assemble JSON for Socrata
        hasher = hashlib.md5()
        dataRows = []
        rowCount = 0
        chunkCount = SOC_CHUNK
        for line in ourData["data"]:
            if chunkCount == SOC_CHUNK:
                print("Rows %d-%d..." % (rowCount + 1, min(rowCount + SOC_CHUNK, len(ourData["data"]))))
            entry = None
            hashFields = None
            
            # Manage the daily device counter:
            if fileType == "matched" or fileType == "unmatched":
                if line["dev_addr"] not in addrLookup:
                    addrLookupCounter += 1
                    addrLookup[line["dev_addr"]] = addrLookupCounter
            
            if fileType == "traf_match_summary":
                entry = {"origin_reader_identifier": devices[line["origin_device_id"]]["device_name"],
                         "destination_reader_identifier": devices[line["dest_device_id"]]["device_name"],
                         "origin_roadway": line["origin_road"],
                         "origin_cross_street": line["origin_cross_st"],
                         "origin_direction": line["origin_dir"],
                         "destination_roadway": line["dest_road"],
                         "destination_cross_street": line["dest_cross_st"],
                         "destination_direction": line["dest_dir"],
                         "segment_length_miles": line["seg_length"],
                         "timestamp": socTime(line["timestamp"]),
                         "average_travel_time_seconds": line["avg_travel_time"],
                         "average_speed_mph": line["avg_speed"],
                         "summary_interval_minutes": line["interval"],
                         "number_samples": line["samples"],
                         "standard_deviation": line["std_dev"]
                    }
                hashFields = ["timestamp", "origin_reader_identifier", "destination_reader_identifier", "segment_length_miles"]
            elif fileType == "matched":
                entry = {"device_address": addrLookup[line["dev_addr"]], # This is a daily incrementing counter per John's suggestion.
                         "origin_reader_identifier": devices[line["origin_device_id"]]["device_name"],
                         "destination_reader_identifier": devices[line["dest_device_id"]]["device_name"],
                         "travel_time_seconds": line["travel_time_secs"],
                         "speed_miles_per_hour": line["speed"],
                         "match_validity": line["match_validity"],
                         "filter_identifier": line["filter_id"],
                         "start_time": socTime(line["start_time"]),
                         "end_time": socTime(line["end_time"]),
                         "day_of_week": arrow.get(line["start_time"]).format("dddd")
                    }
                hashFields = ["start_time", "end_time", "origin_reader_identifier", "destination_reader_identifier", "device_address"] 
            elif fileType == "unmatched":
                entry = {"host_read_time": socTime(line["host_timestamp"]),
                         "field_device_read_time": socTime(line["field_timestamp"]),
                         "reader_identifier": devices[line["device_id"]]["device_name"],
                         "device_address": addrLookup[line["dev_addr"]] # TODO: Replace with randomized MAC address?
                    }
                hashFields = ["host_read_time", "reader_identifier", "device_address"]

            hashStr = "".join([str(entry[q]) for q in hashFields])
            hasher.update(hashStr.encode("utf-8"))
            entry["record_id"] = hasher.hexdigest()
                    
            dataRows.append(entry)
            
            chunkCount -= 1
            rowCount += 1
            if chunkCount == 0 or rowCount == len(ourData["data"]): 
                # Push to Socrata
                s = socratautil.Soda(auth=config.SOC_WRITE_AUTH, records=dataRows, resource=socResource, location_field=None, source="datalake")
                
                dataRows = []
                chunkCount = SOC_CHUNK
        
        # Increment count:
        count += 1
    
    print("Records processed: %d" % count)
    return count    

if __name__ == "__main__":
    main()
