"""
Bluetooth sensor JSON standardization translates between the "raw" and "rawjson" layers.

@author Kenneth Perrine, Nadia Florez
"""
from support import etl_app, last_update, perfmet
from util import date_util
import config

import csv, json, datetime, os

# This sets up application information and command line parameters:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="bt_json_standard.py",
    appDescr="Performs JSON canonicalization for Bluetooth data between the raw and rawjson Data Lake buckets")

class BTJSONStandardApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Bluetooth JSON canonicalization.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.sourceDir = None
        super().__init__("bt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="raw",
                         purposeTgt="rawjson",
                         perfmetStage="Standardize")
    
    def etlActivity(self, processingDate, runCount):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for Bluetooth:
        unitDataProv = config.createUnitDataAccessor(self.dataSource)
        unitData = unitDataProv.retrieve()
                
        # Configure the source and target repositories:
        provSrc = last_update.LastUpdStorageCatProv(self.storageSrc)
        provTgt = last_update.LastUpdStorageCatProv(self.storageTgt)
        comparator = last_update.LastUpdate(provSrc, provTgt).configure(startDate=self.startDate,
                                                                        endDate=self.endDate,
                                                                        baseExtKey=False)
        count = 0
        prevDate = None
        for item in comparator.compare(lastRunDate=self.lastRunDate):
            # Check for valid data files:
            if item.ext not in ("traf_match_summary.txt", "matched.txt", "unmatched.txt"):
                print("WARNING: Unsupported file type or extension: %s" % item.ext)
                continue
            
            # Cause the catalog to update only after we complete all records for each date:
            if not prevDate:
                # This runs on the first item encountered. Write unit data to the target repository:
                config.createUnitDataAccessor(self.storageTgt).store(unitData)
                prevDate = item.date                
            if item.date != prevDate:
                self.storageTgt.flushCatalog()
            
            # Read in the file and call the transformation code.
            print("%s: %s -> %s" % (item.payload["path"], self.stroageSrc.repository, self.storageTgt.repository))
            filepathSrc = self.storageSrc.retrieveFilePath(item.payload["path"])
            fileType = item.identifier.ext.split(".")[0] # Get string up to the file type extension.
            outJSON, perfWork = self.btStandardize(item, unitData, filepathSrc,
                self.storageTgt.makeFilename(item.identifier.base, fileType + ".json", item.identifier.date), fileType, processingDate)

            # Clean up:
            os.remove(filepathSrc)
            
            # Prepare for writing to the target:
            catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, fileType + ".json",
                                                                  item.identifier.date, processingDate)
            self.storageTgt.writeBuffer(outJSON, catalogElement, cacheCatalogFlag=True)
            
            # Final stages:
            self.perfmet.recordCollect(item.identifier.date, representsDay=True)
            if fileType == "unmatched":
                for sensor, rec in perfWork.items():
                    self.perfmet.recordSensorObs(sensor, "Unmatched Entries", perfmet.SensorObs(observation=rec[0],
                        expected=None, collectionDate=item.identifier.date, minTimestamp=rec[1], maxTimestamp=rec[2]))
                # TODO: One issue we have with this is that there isn't a definitive way to know if a BT sensor is dead
                # without it totally missing from the list. We need to add in zero-entries for sensors we expect to be working.
                # Or, calculate expectations using prior records.
                self.perfmet.writeSensorObs()

            count += 1
        else:
            self.storageTgt.flushCatalog()
        
        self.perfmet.logJob(count)
        print("Records processed: %d" % count)
        return count    

def _parseTime(inTime):
    "Parses the time string as encountered in the Bluetooth source files."
    
    try:
        return date_util.localize(datetime.datetime.strptime(inTime, "%m/%d/%Y %I:%M:%S %p"))
    except (ValueError, TypeError):
        return None

def _parseTimeShort(inTime):
    "Parses the time string as encountered in the Bluetooth source files."
    
    try:
        return date_util.localize(datetime.datetime.strptime(inTime, "%m/%d/%Y %I:%M %p"))
    except (ValueError, TypeError):
        return None

def btStandardize(storageItem, filepathSrc, filenameTgt, fileType, processingDate):
    """
    Performs the actual Bluetooth standardization. Retrns data buffer and performance metrics work.
    """
    # Step 1: Define data columns:
    if fileType == "unmatched":
        btDataColumns = ["host_timestamp", "ip_address", "field_timestamp",
                       "reader_id", "dev_addr"]
        btDateColumns = (["host_timestamp", "field_timestamp"], _parseTime)
    elif fileType == "matched":
        btDataColumns = ["dev_addr", "origin_reader_id", "dest_reader_id",
                        "start_time", "end_time", "travel_time_secs", "speed",
                        "match_validity", "filter_id"]
        btDateColumns = (["start_time", "end_time"], _parseTime)
    elif fileType == "traf_match_summary":
        btDataColumns = ["origin_reader_id", "dest_reader_id", "origin_road", "origin_cross_st",
                           "origin_dir", "dest_road", "dest_cross_st", "dest_dir", "seg_length",
                           "timestamp", "avg_travel_time", "avg_speed", "interval", "samples",
                           "std_dev"]
        btDateColumns = (["timestamp"], _parseTimeShort)

    # Step 2: Define header:
    jsonHeader = {"data_type": "bluetooth",
                  "file_type": fileType,
                  "origin_filename": os.path.basename(filepathSrc),
                  "target_filename": filenameTgt,
                  "collection_date": str(storageItem.identifier.date),
                  "processing_date": str(processingDate)}

    # Step 3: Read in the file and parse dates as we read:
    data = []
    perfWork = {} # This will be sensor -> [count, minTime, maxTime]
    reader = csv.DictReader(open(filepathSrc), fieldnames=btDataColumns)
    for row in reader:
        for col in btDateColumns[0]:
            row[col] = btDateColumns[1](row[col])
        data.append(row)
        
        # Performance metrics:
        if fileType == "unmatched":
            if row["reader_id"] and str(row["reader_id"] == "nan"):
                if row["reader_id"] not in perfWork:
                    perfWork[row["reader_id"]] = [0, row["host_timestamp"], row["host_timestamp"]]
                recs = perfWork[row["reader_id"]]
                recs[0] += 1
                if row["host_timestamp"]:
                    if row["host_timestamp"] < recs[1]:
                        recs[1] = row["host_timestamp"]
                    elif row["host_timestamp"] > recs[2]:
                        recs[2] = row["host_timestamp"]
            
    # We're complete!
    reader.close()
    return {"header": jsonHeader,
            "data": json.dumps(data)}, perfWork 

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = BTJSONStandardApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
