"""
Publish Bluetooth "Ready" Data Lake data

@author Kenneth Perrine, Nadia Florez
"""
from support import etl_app, last_update
from drivers import publish_socrata
import config

import arrow

import json, os, hashlib

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
                                   last_update.LastUpdCatProv(self.storageTgt, config.getRepository("public")),
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
        
        # Read in the file and call the transformation code.
        print("%s: %s -> %s" % (item.payload["path"], self.stroageSrc.repository, self.storageTgt.repository))
        filepathSrc = self.storageSrc.retrieveFilePath(item.payload["path"])
        fileType = item.identifier.ext.split(".")[0] # Get string up to the file type extension.
        with open(filepathSrc, "r") as fileObj:
            data = json.loads(fileObj)

        # Clean up:
        os.remove(filepathSrc)
        
        # These variables will keep track of the device counter that gets reset daily:
        if item.date != self.prevDate:
            self.addrLookup = {}
            self.addrLookupCounter = 0
        
        # Generate device lookup:
        devices = {d["device_id"]: d for d in data["devices"]}
        
        # Assemble JSON for Socrata
        hasher = hashlib.md5()
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
                         "timestamp": publish_socrata.socTime(line["timestamp"]),
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
                         "start_time": publish_socrata.socTime(line["start_time"]),
                         "end_time": publish_socrata.socTime(line["end_time"]),
                         "day_of_week": arrow.get(line["start_time"]).format("dddd")
                    }
                hashFields = ["start_time", "end_time", "origin_reader_identifier", "destination_reader_identifier", "device_address"] 
            elif fileType == "unmatched":
                entry = {"host_read_time": publish_socrata.socTime(line["host_timestamp"]),
                         "field_device_read_time": publish_socrata.socTime(line["field_timestamp"]),
                         "reader_identifier": devices[line["device_id"]]["device_name"],
                         "device_address": self.addrLookup[line["dev_addr"]] # TODO: Replace with randomized MAC address?
                    }
                hashFields = ["host_read_time", "reader_identifier", "device_address"]

            hashStr = "".join([str(entry[q]) for q in hashFields])
            hasher.update(hashStr.encode("utf-8"))
            entry["record_id"] = hasher.hexdigest()
            
            publisher.addRow(entry)
        publisher.flush()
        
        # Performance metrics:
        self.perfmet.recordCollect(item.date, representsDay=True)

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
