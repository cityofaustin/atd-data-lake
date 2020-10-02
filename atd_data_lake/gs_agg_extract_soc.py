"""
Publish GRIDSMART Aggregated "Ready" Data Lake data

@author Kenneth Perrine, Nadia Florez
"""
import hashlib

import arrow

import _setpath
from atd_data_lake.support import etl_app, last_update
from atd_data_lake import config

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="gs_agg_extract_soc.py",
    appDescr="Extracts GRIDSMART aggregates from the 'Ready' bucket to Socrata")

class GSAggPublishApp(etl_app.ETLApp):
    """
    Application functions and special behavior around GRIDSMART exporting to Socrata.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("gs", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="ready",
                         perfmetStage="Publish")
        self.publisher = None

    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """
        parser.add_argument("-a", "--agg", type=int, default=15, help="aggregation interval, in minutes (default: 15)")
        parser.add_argument("-u", "--no_unassigned", action="store_true", default=False, help="skip 'unassigned' approaches")

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # Establish the publishers:
        self.publisher = config.createPublisher("gs", None, self.storageSrc.catalog,
                                                simulationMode=self.simulationMode,
                                                writeFilePath=self.writeFilePath)
        
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc, extFilter="agg%d.json" % self.args.agg),
                                   last_update.LastUpdCatProv(self.storageSrc.catalog, config.getRepository("public")),
                                   baseExtKey=False)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.publisher.connector.getIdentifier()))
        data = self.storageSrc.retrieveJSON(item.label)
        device = data["device"] if "device" in data else None
        
        # Contingency for bad device info:
        if not device:
            device = {"atd_device_id": None,
                      "primary_st": data["site"]["site"]["Location"]["Street1"],
                      "cross_st": data["site"]["site"]["Location"]["Street2"]}
            print("WARNING: Device for %s / %s has no device information. Skipping." % (device["primary_st"], device["cross_st"]))
            return 0 # Comment this out if we're to record the site information after all.

        # Assemble JSON for the publisher:
        hasher = hashlib.md5()
        errDup = {}
        for line in data["data"]:            
            approach = line["zone_approach"]
            if approach == "Southbound":
                approach = "SOUTHBOUND"
            elif approach == "Northbound":
                approach = "NORTHBOUND"
            elif approach == "Eastbound":
                approach = "EASTBOUND"
            elif approach == "Westbound":
                approach = "WESTBOUND"
            elif approach == "Unassigned" and not self.args.no_unassigned:
                approach = "UNASSIGNED"
                _addErrDup(errDup, "WARNING: Approach is UNASSIGNED. Including.")
            else:
                _addErrDup(errDup, "WARNING: Approach is %s. Skipping." % approach)
                continue
                
            movement = line["turn"]
            if movement == "S":
                movement = "THRU"
            elif movement == "L":
                movement = "LEFT TURN"
            elif movement == "R":
                movement = "RIGHT TURN"
            elif movement == "U":
                movement = "U-TURN"
            else:
                _addErrDup(errDup, "WARNING: Movement is %s" % movement)
            
            timestamp = arrow.get(line["timestamp"])
                
            entry = {"atd_device_id": device["atd_device_id"],
                     "read_date": self.publisher.convertTime(timestamp.datetime),
                     "intersection_name": device["primary_st"].strip() + " / " + device["cross_st"].strip(),
                     "direction": approach,
                     "movement": movement,
                     "heavy_vehicle": line["heavy_vehicle"] != 0,
                     "volume": line["volume"],
                     "speed_average": line["speed_avg"],
                     "speed_stddev": line["speed_std"],
                     "seconds_in_zone_average": line["seconds_in_zone_avg"],
                     "seconds_in_zone_stddev": line["seconds_in_zone_std"],
                     "month": timestamp.month,
                     "day": timestamp.day,
                     "year": timestamp.year,
                     "hour": timestamp.hour,
                     "minute": timestamp.minute,
                     "day_of_week": (timestamp.weekday() + 1) % 7,
                     "bin_duration": self.args.agg * 60}
            hashFields = ["intersection_name", "read_date", "heavy_vehicle", "direction", "movement"]

            hashStr = "".join([str(entry[q]) for q in hashFields])
            hasher.update(hashStr.encode("utf-8"))
            entry["record_id"] = hasher.hexdigest()

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

        # Output warnings:
        for errMsg in errDup:
            print(errMsg + ": (x%d)" % errDup[errMsg])
        
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
