"""
Bluetooth sensor JSON preparation translates "rawjson" and "ready" layers.

@author Kenneth Perrine, Nadia Florez
"""
from support import etl_app, last_update
import config

import pandas as pd

import json, os, hashlib

# This sets up application information and command line parameters:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="bt_ready.py",
    appDescr="Performs JSON enrichment for Bluetooth data between the 'rawjson' and 'ready' Data Lake buckets")

class BTReadyApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Bluetooth JSON final data enrichment.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.sourceDir = None
        super().__init__("bt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="rawjson",
                         purposeTgt="ready",
                         perfmetStage="Ready")

    def etlActivity(self, processingDate, runCount):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for Bluetooth:
        unitDataProv = config.createUnitDataAccessor(self.storageSrc)
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
            if item.ext not in ("traf_match_summary.json", "matched.json", "unmatched.json"):
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
            outJSON = btReady(item, unitData, filepathSrc, fileType, processingDate)

            # Clean up:
            os.remove(filepathSrc)

            # Prepare for writing to the target:
            catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, fileType + ".json",
                                                                  item.identifier.date, processingDate)
            self.storageTgt.writeJSON(outJSON, catalogElement, cacheCatalogFlag=True)

            count += 1
        else:
            self.storageTgt.flushCatalog()
        
        self.perfmet.logJob(count)
        print("Records processed: %d" % count)
        return count    

def _createHash(row):
    """
    Returns a hash that's based upon a row's contents from the data file
    """
    toHash = row['device_type'] + row['device_ip'] + str(row['lat']) + str(row['lon'])
    hasher = hashlib.md5()
    hasher.update(bytes(toHash, "utf-8"))
    return hasher.hexdigest()

def btReady(unitData, filepathSrc, fileType, processingDate):
    """
    Transforms Bluetooth data to "ready" JSON along with the unit data.
    """
    # Step 1: Read the file:
    with open(filepathSrc, 'r') as dataFile:
        data = json.load(dataFile)
        
    # Step 2: Prepare header:
    header = data["header"]
    header["processing_date"] = str(processingDate)    

    # Step 3: Convert the data and devices to Pandas dataframes:
    data = pd.DataFrame(data["data"])
    devices = pd.DataFrame(unitData["devices"])
    
    # Step 3: Tie device information to data rows:
    devices['device_id'] = devices.apply(_createHash, axis=1)    
    if fileType == "unmatched":
        data = data.merge(devices[['device_name', 'device_id']],
                          left_on='reader_id', right_on='device_name', how='inner') \
                            .drop(columns='device_name')
        # TODO: Consider removing "reader_id" here, for memory efficiency.
        devices = devices[devices.device_id.isin(data.device_id.unique())]
        devices = devices.apply(lambda x: x.to_dict(), axis=1).tolist()
    elif fileType == "matched" or fileType == "traf_match_summary":
        data = data.merge(devices[['device_name', 'device_id']],
                                   left_on='origin_reader_id', right_on='device_name', how='inner') \
                                    .drop(columns='device_name').rename(columns={"device_id": "origin_device_id"})
        data = data.merge(devices[['device_name', 'device_id']],
                                   left_on='dest_reader_id', right_on='device_name', how='inner') \
                                    .drop(columns='device_name').rename(columns={"device_id": "dest_device_id"})
        # TODO: Consider removing "origin_reader_id" and "dest_reader_id" here, for memory efficiency.
        devices = devices[devices.device_id.isin(data.origin_device_id
                                    .append(data.dest_device_id, ignore_index=True).unique())]
        devices = devices.apply(lambda x: x.to_dict(), axis=1).tolist()
    
    # Step 4: Prepare the final data JSON buffer:
    data = data.apply(lambda x: x.to_dict(), axis=1).tolist()
    jsonized = {'header': header,
                'data': data,
                'devices': devices}
    return jsonized

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
