"""
Bluetooth sensor JSON preparation translates "rawjson" and "ready" layers.

@author Kenneth Perrine, Nadia Florez
"""
from support import etl_app, last_update
import config

import pandas as pd

import json, os, hashlib

# This sets up application information:
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
        super().__init__("bt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="rawjson",
                         purposeTgt="ready",
                         perfmetStage="Ready")
        self.unitData = None

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for Bluetooth:
        unitDataProv = config.createUnitDataAccessor(self.storageSrc)
        self.unitData = unitDataProv.retrieve()
        
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc),
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=False)
        self.perfmet.logJob(count)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Check for valid data files:
        if item.ext not in ("traf_match_summary.json", "matched.json", "unmatched.json"):
            print("WARNING: Unsupported file type or extension: %s" % item.ext)
            return 0
        
        # Write unit data to the target repository:
        if self.itemCount == 0:
            config.createUnitDataAccessor(self.storageTgt).store(self.unitData)
        
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.payload["path"], self.stroageSrc.repository, self.storageTgt.repository))
        filepathSrc = self.storageSrc.retrieveFilePath(item.payload["path"])
        fileType = item.identifier.ext.split(".")[0] # Get string up to the file type extension.
        outJSON = btReady(item, self.unitData, filepathSrc, fileType, self.processingDate)

        # Clean up:
        os.remove(filepathSrc)

        # Prepare for writing to the target:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, fileType + ".json",
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeJSON(outJSON, catalogElement)

        # Performance metrics:
        self.perfmet.recordCollect(item.date, representsDay=True)
        
        return 1

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
    curApp = BTReadyApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
