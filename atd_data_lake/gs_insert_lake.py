"""
Movement of GRIDSMART data files to S3 "raw" layer.

@author Kenneth Perrine, Nadia Florez
"""
import os

import _setpath
from atd_data_lake.support import etl_app, last_update
from atd_data_lake import config
from atd_data_lake.drivers.devices import last_upd_gs, gs_support

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="gs_insert_lake.py",
    appDescr="Inserts GRIDSMART data from field devices into the Raw Data Lake")

class GSInsertLakeApp(etl_app.ETLApp):
    """
    Application functions and special behavior around GRIDSMART ingestion.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.deviceFilter = None
        super().__init__("gs", APP_DESCRIPTION,
                         args=args,
                         purposeTgt="raw",
                         needsTempDir=True,
                         perfmetStage="Ingest")
        self.unitData = None
        self.siteFiles = set()
        self.gsProvider = None

    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """        
        parser.add_argument("-f", "--name_filter", default=".*", help="filter processing on units whose names match the given regexp")

    def _ingestArgs(self, args):
        """
        Processes application-specific variables
        """
        if hasattr(args, "name_filter"):
            self.deviceFilter = args.name_filter
        super()._ingestArgs(args)
    
    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for GRIDSMART:
        unitDataProv = config.createUnitDataAccessor(self.dataSource)
        self.unitData = unitDataProv.retrieve()
        deviceLogreaders = gs_support.getDevicesLogreaders(self.unitData, self.deviceFilter)
                
        # Configure the source and target repositories and start the compare loop:
        self.gsProvider = last_upd_gs.LastUpdGSProv(deviceLogreaders, self.tempDir)
        count = self.doCompareLoop(self.gsProvider,
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
        
        # Put together the site file:
        self._insertSiteFile(item, item.provItem.payload)
        
        # Obtain the raw count data archive:
        countsFilePath = self.gsProvider.resolvePayload(item)
        if not countsFilePath:
            return 0
        
        # Write raw count data archive to storage:
        print("%s -> %s" % (item.label, self.storageTgt.repository))
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, item.identifier.ext,
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeFile(countsFilePath, catalogElement, cacheCatalogFlag=True)
        
        # Clean up:
        os.remove(countsFilePath)
        
        # Performance metrics:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)

        return 1

    def _insertSiteFile(self, item, deviceLogreader):
        """
        Handles the construction and archiving of the site file.
        
        @param item: Of type last_update.LastUpdate._LastUpdItem.
        @param device: Of type gs_support._GSDevice
        """
        # For the time being, unit data and site files will be stored for the day that the 
        # collection happens, whereas the collected data is for the previous day.
        ourDay = self.processingDate.replace(hour=0, minute=0, second=0, microsecond=0) # Same day
        siteFilename = "{}_site_{}.json".format(item.identifier.base, ourDay.strftime("%Y-%m-%d"))
        if siteFilename in self.siteFiles:
            return
        print("%s -> %s" % (siteFilename, self.storageTgt.repository))
        
        # Arrange the JSON:
        header = {"data_type": "gs_site",
                  "target_filename": siteFilename,
                  "collection_date": str(ourDay),
                  "device_net_addr": deviceLogreader.device.netAddr}
        jsonData = {'header': header,
                     'site': deviceLogreader.site,
                     'datetime': deviceLogreader.timeFile,
                     'hardware_info': deviceLogreader.hwInfo}
        
        # Write it out:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, "site.json",
                                                              ourDay, self.processingDate)
        self.storageTgt.writeJSON(jsonData, catalogElement, cacheCatalogFlag=True)
        self.siteFiles.add(siteFilename)

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = GSInsertLakeApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
