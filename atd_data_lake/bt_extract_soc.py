"""
Publish Bluetooth "Ready" Data Lake data

@author Kenneth Perrine, Nadia Florez
"""
import hashlib

import arrow

import _setpath
from atd_data_lake.support import etl_app, last_update
from atd_data_lake import config

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="bt_extract_soc.py",
    appDescr="Extracts Bluetooth files from the 'Ready' bucket to Socrata")

class BTPublishApp(etl_app.ETLApp):
    """
    Application functions and special behavior around Bluetooth exporting to Socrata.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("bt", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="ready",
                         perfmetStage="Publish")
        self.publishers = None
        self.addrLookup = {}
        self.addrLookupCounter = 0

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # Establish the publishers:
        self.publishers = {"traf_match_summary": config.createPublisher("bt", "traf_match_summary", self.storageSrc.catalog,
                                                                        simulationMode=self.simulationMode,
                                                                        writeFilePath=self.writeFilePath),
                           "matched": config.createPublisher("bt", "matched", self.storageSrc.catalog,
                                                             simulationMode=self.simulationMode,
                                                             writeFilePath=self.writeFilePath),
                           "unmatched": config.createPublisher("bt", "unmatched", self.storageSrc.catalog,
                                                               simulationMode=self.simulationMode,
                                                               writeFilePath=self.writeFilePath)}
        
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc),
                                   last_update.LastUpdCatProv(self.storageSrc.catalog, config.getRepository("public")),
                                   baseExtKey=False)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Check for valid data files:
        if item.identifier.ext not in ("traf_match_summary.json", "matched.json", "unmatched.json"):
            print("WARNING: Unsupported file type or extension: %s" % item.identifier.ext)
            return 0
        
        # Read in the file and call the transformation code.
        fileType = item.identifier.ext.split(".")[0] # Get string up to the file type extension.
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.publishers[fileType].connector.getIdentifier()))
        data = self.storageSrc.retrieveJSON(item.label)
        
        # These variables will keep track of the device counter that gets reset daily:
        if item.identifier.date != self.prevDate:
            self.addrLookup = {}
            self.addrLookupCounter = 0
        
        # Generate device lookup:
        devices = {d["device_id"]: d for d in data["devices"]}
        
        # Assemble JSON for Socrata
        publisher = self.publishers[fileType]
        for line in data["data"]:
            entry = None
            hashFields = None
            
            # Manage the daily device counter:
            if fileType == "matched" or fileType == "unmatched":
                if line["dev_addr"] not in self.addrLookup:
                    self.addrLookupCounter += 1
                    self.addrLookup[line["dev_addr"]] = self.addrLookupCounter
            
            if fileType == "traf_match_summary":
                entry = {"origin_reader_identifier": devices[line["origin_device_id"]]["device_name"],
                         "destination_reader_identifier": devices[line["dest_device_id"]]["device_name"],
                         "origin_roadway": line["origin_road"],
                         "origin_cross_street": line["origin_cross_st"],
                         "origin_direction": line["origin_dir"],
                         "destination_roadway": line["dest_road"],
                         "destination_cross_street": line["dest_cross_st"],
                         "destination_direction": line["dest_dir"],
                         "segment_length_miles": line["seg_length"],
                         "timestamp": publisher.convertTime(arrow.get(line["timestamp"]).datetime),
                         "average_travel_time_seconds": line["avg_travel_time"],
                         "average_speed_mph": line["avg_speed"],
                         "summary_interval_minutes": line["interval"],
                         "number_samples": line["samples"],
                         "standard_deviation": line["std_dev"]
                    }
                hashFields = ["timestamp", "origin_reader_identifier", "destination_reader_identifier", "segment_length_miles"]
            elif fileType == "matched":
                entry = {"device_address": self.addrLookup[line["dev_addr"]], # This is a daily incrementing counter per John's suggestion.
                         "origin_reader_identifier": devices[line["origin_device_id"]]["device_name"],
                         "destination_reader_identifier": devices[line["dest_device_id"]]["device_name"],
                         "travel_time_seconds": line["travel_time_secs"],
                         "speed_miles_per_hour": line["speed"],
                         "match_validity": line["match_validity"],
                         "filter_identifier": line["filter_id"],
                         "start_time": publisher.convertTime(arrow.get(line["start_time"]).datetime),
                         "end_time": publisher.convertTime(arrow.get(line["end_time"]).datetime),
                         "day_of_week": arrow.get(line["start_time"]).format("dddd")
                    }
                hashFields = ["start_time", "end_time", "origin_reader_identifier", "destination_reader_identifier", "device_address"] 
            elif fileType == "unmatched":
                entry = {"host_read_time": publisher.convertTime(arrow.get(line["host_timestamp"]).datetime),
                         "field_device_read_time": publisher.convertTime(arrow.get(line["field_timestamp"]).datetime),
                         "reader_identifier": devices[line["device_id"]]["device_name"],
                         "device_address": self.addrLookup[line["dev_addr"]] # TODO: Replace with randomized MAC address?
                    }
                hashFields = ["host_read_time", "reader_identifier", "device_address"]

            hashStr = "".join([str(entry[q]) for q in hashFields])
            hasher = hashlib.md5()
            hasher.update(hashStr.encode("utf-8"))
            entry["record_id"] = hasher.hexdigest()
            
            publisher.addRow(entry)
        publisher.flush()
        publisher.reset()
        
        # Write to catalog:
        if not self.simulationMode:
            catElement = self.catalog.buildCatalogElement(config.getRepository("public"), item.identifier.base,
                                                          item.identifier.ext, item.identifier.date,
                                                          self.processingDate, publisher.connector.getIdentifier())
            self.catalog.upsert(catElement)
        
        # Performance metrics:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)

        return 1

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = BTPublishApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
