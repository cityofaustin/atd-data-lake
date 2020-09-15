"""
Wavetronix sensor JSON standardization translates between the "raw" and "rawjson" layers.

@author Kenneth Perrine, Nadia Florez
"""
from support import etl_app, last_update, perfmet
from util import date_util
import config

import csv, os, datetime

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="wt_json_standard.py",
    appDescr="Performs JSON canonicalization for Wavetronix data between the raw and rawjson Data Lake buckets")

class WTJSONStandardApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Wavetronix JSON canonicalization.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("wt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="raw",
                         purposeTgt="standardized",
                         perfmetStage="Standardize")
        self.unitData = None
    
    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for Wavetronix:
        unitDataProv = config.createUnitDataAccessor(self.dataSource)
        self.unitData = unitDataProv.retrieve()
                
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc),
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=False)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Write unit data to the target repository:
        if self.itemCount == 0:
            config.createUnitDataAccessor(self.storageTgt).store(self.unitData)
            
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.storageTgt.repository))
        filepathSrc = self.storageSrc.retrieveFilePath(item.label)
        outJSON, perfWork = wtStandardize(item, filepathSrc,
            self.storageTgt.makeFilename(item.identifier.base, "json", item.identifier.date), self.processingDate)

        # Clean up:
        os.remove(filepathSrc)
        
        # Prepare for writing to the target:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, "json",
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeJSON(outJSON, catalogElement)
            
        # Final stages:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)
        for sensor, rec in perfWork.items():
            self.perfmet.recordSensorObs(sensor, "Vehicle Counts", perfmet.SensorObs(observation=rec[0],
                expected=None, collectionDate=item.identifier.date, minTimestamp=rec[1], maxTimestamp=rec[2]))
        self.perfmet.writeSensorObs()

        return 1

def wtStandardize(storageItem, filepathSrc, filenameTgt, processingDate):
    """
    Performs the actual Wavetronix standardization, which is basically doing a direct translation from CSV to JSON
    """
    # Define header:
    jsonHeader = {"data_type": "wavetronix",
                  "origin_filename": os.path.basename(filepathSrc),
                  "target_filename": filenameTgt,
                  "collection_date": str(storageItem.identifier.date),
                  "processing_date": str(processingDate)}

    # Read in the file:
    data = []
    perfWork = {} # This will be sensor -> [count, minTime, maxTime]
    with open(filepathSrc, "rt") as fileReader:
        reader = csv.DictReader(fileReader)
        for row in reader:
            data.append({"detID": int(row["detID"]),
                         "intID": int(row["intID"]),
                         "curDateTime": str(date_util.localize(datetime.datetime.strptime(row["curDateTime"], "%Y-%m-%d %H:%M:%S"))),
                         "intName": row["intName"],
                         "detName": row["detName"],
                         "volume": int(row["volume"]),
                         "occupancy": int(row["occupancy"]),
                         "speed": int(row["speed"]),
                         "status": row["status"],
                         "uploadSuccess": int(row["uploadSuccess"]),
                         "detCountComparison": int(row["detCountComparison"]),
                         "dailyCumulative": int(row["dailyCumulative"])})
            
            # Performance metrics:
            if row["intName"] and str(row["intName"] != "nan"):
                if row["intName"] not in perfWork:
                    perfWork[row["intName"]] = [0, row["curDateTime"], row["curDateTime"]]
                recs = perfWork[row["intName"]]
                recs[0] += int(row["volume"])
                if row["curDateTime"]:
                    if row["curDateTime"] < recs[1]:
                        recs[1] = row["curDateTime"]
                    elif row["curDateTime"] > recs[2]:
                        recs[2] = row["curDateTime"]
                
    # We're complete!
    ret = {"header": jsonHeader,
           "data": data}
    return ret, perfWork 

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = WTJSONStandardApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
