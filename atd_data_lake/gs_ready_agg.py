"""
ATD Data Lake 'ready' bucket for GRIDSMART, with aggregation

@author Kenneth Perrine, Nadia Florez
"""
import pandas as pd
import numpy as np
import arrow

import _setpath
from atd_data_lake.support import etl_app, last_update
from atd_data_lake.util import date_util

APP_DESCRIPTION = etl_app.AppDescription(
    appName="gs_ready_agg.py",
    appDescr="Aggregates 'ready' Data Lake bucket GRIDSMART counts")

class GSReadyAggApp(etl_app.ETLApp):
    """
    Application functions and special behavior around GRIDSMART aggregation
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("gs", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="ready",
                         purposeTgt="ready",
                         perfmetStage="Aggregate")

    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """
        parser.add_argument("-a", "--agg", type=int, default=15, help="aggregation interval, in minutes (default: 15)")
    
    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc, extFilter="counts.json"),
                                   last_update.LastUpdStorageCatProv(self.storageTgt, extFilter="agg%d.json" % self.args.agg),
                                   baseExtKey=False)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        print("%s: %s" % (item.label, self.storageSrc.repository))
        data = self.storageSrc.retrieveJSON(item.label)
        header = data["header"]
        
        # Collect movement information:
        movements = []
        for camera in data["site"]["site"]["CameraDevices"]:
            for zoneMask in camera["Fisheye"]["CameraMasks"]["ZoneMasks"]:
                if "Vehicle" in zoneMask:
                    movements.append({"zone_approach": zoneMask["Vehicle"]["ApproachType"],
                                      "turn_type": zoneMask["Vehicle"]["TurnType"],
                                      "zone": zoneMask["Vehicle"]["Id"]})
        
        # Process the counts:
        countData = pd.DataFrame(data["counts"])
        countData['heavy_vehicle'] = np.where(countData.vehicle_length < 17, 0, 1)
        # In the following line, we convert to UTC because there's a bug in the grouper that doesn't deal with
        # the end of daylight savings time.
        countData['timestamp'] = pd.to_datetime(countData["timestamp_adj"], utc=True)
        countData = countData.merge(pd.DataFrame(movements), on='zone')

        # Do the grouping:        
        colValues = [pd.Grouper(key='timestamp', freq=('%ds' % (self.args.agg * 60))), 'zone_approach', 'turn', 'heavy_vehicle']
        grouped = countData.groupby(colValues)
        volume = grouped.size().reset_index(name='volume')
        avgSpeed = grouped.agg({'speed': 'mean'}).round(3).reset_index().rename(columns={'speed': 'speed_avg'})
        stdSpeed = grouped.agg({'speed': 'std'}).fillna(0).round(3).reset_index().rename(columns={'speed': 'speed_std'})
        avgSecInZone = grouped.agg({'seconds_in_zone': 'mean'}).round(3).reset_index().rename(columns={'seconds_in_zone': 'seconds_in_zone_avg'})
        stdSecInZone = grouped.agg({'seconds_in_zone': 'std'}).round(3).fillna(0).reset_index().rename(columns={'seconds_in_zone': 'seconds_in_zone_std'})

        # Merging all information
        colValues[0] = "timestamp"
        summarized = volume.merge(avgSpeed, on=colValues).merge(stdSpeed, on=colValues).merge(avgSecInZone, on=colValues).merge(stdSecInZone, on=colValues)
        summarized = summarized[['timestamp', 'zone_approach', 'turn', 'heavy_vehicle',
                                'volume', 'speed_avg', 'speed_std', 'seconds_in_zone_avg', 'seconds_in_zone_std']]
        # While converting the timestamp to a string, we also convert it back to our local time zone to counter
        # the grouping/UTC workaround that was performed above.
        summarized["timestamp"] = summarized["timestamp"].dt.tz_convert(date_util.LOCAL_TIMEZONE).astype(str)
        
        # Update the header
        header["processing_date"] = str(date_util.localize(arrow.now().datetime))
        header["agg_interval_sec"] = self.args.agg * 60
        
        # Assemble together the aggregation file:
        newFileContents = {"header": header,
                           "data": summarized.apply(lambda x: x.to_dict(), axis=1).tolist(),
                           "site": data["site"],
                           "device": data["device"]}
        
        # Write the aggregation:
        catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, "agg%d.json" % self.args.agg,
                                                              item.identifier.date, self.processingDate)
        self.storageTgt.writeJSON(newFileContents, catalogElement)
            
        # Performance metrics logging:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)
        
        return 1

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = GSReadyAggApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
