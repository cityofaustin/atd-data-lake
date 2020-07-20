"""
Bluetooth sensor ingestion takes files from the AWAM share and places them into the Data Lake
"raw" layer.

@author Kenneth Perrine
"""
from support import etl_app, last_update
from config import config_app
from util import date_dirs
from drivers import last_upd_fs

# This sets up application information and command line parameters:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="bt_insert_lake.py",
    appDescr="Inserts Bluetooth data from AWAM share into the Raw Data Lake")

# This defines the valid filename formats that exist in the AWAM directory:
DIR_DEFS = [date_dirs.DateDirDef(prefix=config_app.UNIT_LOCATION + "_bt_",
                                 dateFormat="%m-%d-%Y",
                                 postfix=".txt"),
            date_dirs.DateDirDef(prefix=config_app.UNIT_LOCATION + "_btmatch_",
                                 dateFormat="%m-%d-%Y",
                                 postfix=".txt"),
            date_dirs.DateDirDef(prefix=config_app.UNIT_LOCATION + "_bt_summary_15_",
                                 dateFormat="%m-%d-%Y",
                                 postfix=".txt")]

class BTLastUpdateProv(last_upd_fs.LastUpdFileProv):
    """
    Overrides the default file provider so as to generate the correct identifier for AWAM files
    """
    def _getIdentifier(self, filePath, typeIndex, date):
        """
        Creates identifier for the comparison purposes from the given file information
        """
        # We want our identifiers to be phrased like: Base="Austin"; Ext="unmatched.txt".
        prefix = self.pattList[typeIndex].prefix.split("_")[0] # TODO: Returns "Austin"
        postfix = self.pattList[typeIndex].postfix
        if typeIndex == 0:
            desc = "unmatched"
        elif typeIndex == 1:
            desc = "matched"
        elif typeIndex == 2:
            desc = "traf_match_summary"
        else:
            raise ValueError("Bad typeIndex")
        ext = desc + postfix
        return prefix, ext, date

class BTInsertLakeApp(etl_app.ETLApp):
    """
    Special behavior around Bluetooth ingestion. This may seem like overkill, but demonstrates
    how new application-specific variables can be added to the App class.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.sourceDir = None
        super().__init__("bt", APP_DESCRIPTION,
                         args=args,
                         purposeTgt="raw",
                         needsTempDir=False,
                         perfmetStage="Ingest")
    
    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """
        parser.add_argument("-d", "--sourcedir", default=".", help="Source directory (e.g. AWAM share) to read Bluetooth files from")
        
    def _ingestArgs(self, args):
        """
        Processes application-specific variables
        """
        self.sourceDir = args.source_dir
        super()._ingestArgs(args)

    def etlActivity(self, processingDate, runCount):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        provSrc = BTLastUpdateProv(self.sourceDir, DIR_DEFS)
        provTgt = self.storageTgt
        comparator = last_update.LastUpdate(provSrc, provTgt).configure(startDate=self.startDate,
                                                                        endDate=self.endDate,
                                                                        baseExtKey=True)
        count = 0
        prevDate = None
        for item in comparator.compare(lastRunDate=self.lastRunDate):
            # Cause the catalog to update only after we complete all records for each date:
            if not prevDate:
                prevDate = item.date
            if item.date != prevDate:
                self.storageTgt.flushCatalog()
                
            # Set up the storage path for the data item:
            pathTgt = self.storageTgt.makePath(item.identifier.base, item.identifier.ext, item.identifier.date)
            print("%s -> %s:%s" % (item.payload, pathTgt.repository, pathTgt))
            
            # Write the file to storage:
            catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, item.identifier.ext,
                                                                  item.identifier.date, processingDate)
            self.storageTgt.writeFile(item.payload, catalogElement, cacheCatalogFlag=True)
            self.perfmet.recordCollect(item.identifier.date, representsDay=True)
            count += 1
        else:
            self.storageTgt.flushCatalog()
        
        self.perfmet.logJob(count)
        print("Records processed: %d" % count)
        return count    

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = BTInsertLakeApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
