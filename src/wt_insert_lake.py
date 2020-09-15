"""
Movement of Wavetronix database contents to S3 "raw" layer.

@author Kenneth Perrine
"""
from support import etl_app, last_update, last_update_db
from util import date_util
import config
from drivers.devices import wt_mssql_db

import os, csv

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="wt_insert_lake.py",
    appDescr="Inserts Wavetronix data from database into the Raw Data Lake")

class WTInsertLakeApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Wavetronix database ingestion.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.wtProvider = None
        super().__init__("wt", APP_DESCRIPTION,
                         args=args,
                         purposeTgt="raw",
                         needsTempDir=True,
                         perfmetStage="Ingest")

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # Establish database connection for Wavetronix records:
        wtDB = wt_mssql_db.WT_MSSQL_DB()
        
        # Configure the source and target repositories and start the compare loop:
        self.wtProvider = last_update_db.LastUpdDB(wtDB, config.getUnitLocation())
        count = self.doCompareLoop(self.wtProvider,
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=False)
        print("Records processed: %d" % count)
        return count

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Obtain the database records for the day being processed:
        recs = self.wtProvider.resolvePayload(item)
        
        # Dump out the contents to a temporary CSV file:
        tempFilePath = os.path.join(self.tempDir, item.label)
        with open(tempFilePath, "w", newline="") as csvFile:
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(["detID", "intID", "curDateTime", "intName", "detName", "volume", "occupancy", "speed", "status",
                                "uploadSuccess", "detCountComparison", "dailyCumulative"])
            for row in recs:
                csvWriter.writerow([row.detID, row.intID, date_util.localize(row.curDateTime).strftime("%Y-%m-%d %H:%M:%S"),
                                    row.intName, row.detName, row.volume, row.occupancy, row.speed, row.status,
                                    row.uploadSuccess, row.detCountComparison, row.dailyCumulative])
        
        # Write database records:
        print("%s -> %s" % (item.label, self.storageTgt.repository))
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, item.identifier.ext,
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeFile(tempFilePath, catalogElement)
        
        # Clean up:
        os.remove(tempFilePath)
        
        # Performance metrics:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)

        return 1

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = WTInsertLakeApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
