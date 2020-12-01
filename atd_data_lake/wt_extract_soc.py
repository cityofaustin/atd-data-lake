"""
Publish Wavetronix "Ready" Data Lake data

@author Kenneth Perrine
"""
import hashlib

import arrow

import _setpath
from atd_data_lake.support import etl_app, last_update
from atd_data_lake import config

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="wt_extract_soc.py",
    appDescr="Extracts Wavetronix from the 'Ready' bucket to Socrata")

class GSAggPublishApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Wavetronix exporting to Socrata.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("wt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="ready",
                         perfmetStage="Publish")
        self.publisher = None

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # Establish the publishers:
        self.publisher = config.createPublisher("wt", None, self.storageSrc.catalog,
                                                simulationMode=self.simulationMode,
                                                writeFilePath=self.writeFilePath)
        
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc, extFilter="json"),
                                   last_update.LastUpdCatProv(self.storageSrc.catalog, config.getRepository("public")),
                                   baseExtKey=True)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.publisher.connector.getIdentifier()))
        data = self.storageSrc.retrieveJSON(item.label)

        # Assemble JSON for the publisher:
        for line in data["data"]:            
            timestamp = arrow.get(line["curDateTime"])
            direction = line["detName"].split("_")
            direction = direction[0] if direction else ""
                
            entry = {"detid": line["detID"],
                     "int_id": line["intID"],
                     "curdatetime": self.publisher.convertTime(timestamp.datetime),
                     "intname": line["intName"],
                     "detname": line["detName"],
                     "volume": line["volume"],
                     "occupancy": line["occupancy"],
                     "speed": line["speed"],
                     "month": timestamp.month,
                     "day": timestamp.day,
                     "year": timestamp.year,
                     "hour": timestamp.hour,
                     "minute": timestamp.minute,
                     "day_of_week": (timestamp.weekday() + 1) % 7,
                     "timebin": "%02d:%02d" % (timestamp.hour, round(timestamp.minute / 15.0) * 15),
                     "direction": direction}
            hashFields = ["intname", "curdatetime", "detid"]

            hashStr = "".join([str(entry[q]) for q in hashFields])
            hasher = hashlib.md5()
            hasher.update(hashStr.encode("utf-8"))
            entry["row_id"] = hasher.hexdigest()

            self.publisher.addRow(entry)
        
        # Write contents to publisher:
        self.publisher.flush()
        self.publisher.reset()
        
        # Write to catalog:
        if not self.simulationMode:
            catElement = self.catalog.buildCatalogElement(config.getRepository("public"), item.identifier.base,
                                                          item.identifier.ext, item.identifier.date,
                                                          self.processingDate, self.publisher.connector.getIdentifier())
            self.catalog.upsert(catElement)

        # Performance metrics:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)

        return 1

def _addErrDup(errDup, errStr):
    if errStr not in errDup: 
        errDup[errStr] = 0
    errDup[errStr] += 1

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = GSAggPublishApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
