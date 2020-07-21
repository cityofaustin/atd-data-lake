"""
Bluetooth sensor ingestion takes files from the AWAM share and places them into the Data Lake
"raw" layer.

@author Kenneth Perrine
"""
from support import etl_app, last_update
from config import config_app
from util import date_dirs
from drivers import last_upd_fs

# This sets up application information:
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
    Application functions and special behavior around Bluetooth ingestion.
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

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(BTLastUpdateProv(self.sourceDir, DIR_DEFS),
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=True)
        self.perfmet.logJob(count)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Set up the storage path for the data item:
        pathTgt = self.storageTgt.makePath(item.identifier.base, item.identifier.ext, item.identifier.date)
        print("%s -> %s:%s" % (item.payload, pathTgt.repository, pathTgt))
        
        # Write the file to storage:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, item.identifier.ext,
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeFile(item.payload, catalogElement, cacheCatalogFlag=True)
        
        # Performance metrics:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)

        return 1

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
