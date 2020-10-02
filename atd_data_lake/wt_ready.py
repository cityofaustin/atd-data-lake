"""
Bluetooth sensor JSON preparation translates "rawjson" and "ready" layers.

@author Kenneth Perrine, Nadia Florez
"""
import hashlib

import pandas as pd

import _setpath
from atd_data_lake.support import etl_app, last_update
from atd_data_lake import config

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="wt_ready.py",
    appDescr="Performs JSON enrichment for Wavetronix data between the 'rawjson' and 'ready' Data Lake buckets")

class WTReadyApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Wavetronix JSON final data enrichment.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("wt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="standardized",
                         purposeTgt="ready",
                         perfmetStage="Ready")
        self.unitDataProv = None

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for Wavetronix:
        self.unitDataProv = config.createUnitDataAccessor(self.storageSrc)
        self.unitDataProv.prepare(self.startDate, self.endDate)
        
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
        # Check for valid data files:
        if item.identifier.ext == "unit_data.json":
            return 0
        
        # Retrieve unit data closest to the date that we're processing:
        unitData = self.unitDataProv.retrieve(item.identifier.date)
        
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.storageTgt.repository))
        data = self.storageSrc.retrieveJSON(item.label)
        outJSON = wtReady(unitData, data, self.processingDate)

        # Prepare for writing to the target:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, "json",
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeJSON(outJSON, catalogElement)

        # Performance metrics:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)
        
        return 1

def _createHash(row):
    """
    Returns a hash that's based upon a row's contents from the data file
    """
    toHash = str(row['device_type']) + str(row['device_name']) + str(row['device_ip']) + str(row['lat']) + str(row['lon'])
    hasher = hashlib.md5()
    hasher.update(bytes(toHash, "utf-8"))
    return hasher.hexdigest()

def wtReady(unitData, data, processingDate):
    """
    Transforms Wavetronix data to "ready" JSON along with the unit data.
    """
    # Step 1: Prepare header:
    header = data["header"]
    header["processing_date"] = str(processingDate)    

    # Step 2: Convert the data and devices to Pandas dataframes:
    data = pd.DataFrame(data["data"])
    devices = pd.DataFrame(unitData["devices"])
    
    # Step 3: Tie device information to data rows:
    devices['device_id'] = devices.apply(_createHash, axis=1)
    data = data.merge(devices[['kits_id', 'device_id']],
                      left_on='intID', right_on='kits_id', how='inner') \
                      .drop(columns="kits_id")
    data.sort_values(by=["curDateTime", "detID"], inplace=True)
    devices = devices[devices.device_id.isin(data.device_id.unique())]
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
    curApp = WTReadyApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
