"""
JSON standardization for GRIDSMART

@author Kenneth Perrine, Nadia Florez
"""
import os, datetime, json

import pandas as pd
import pytz

import _setpath
from atd_data_lake.support import etl_app, last_update, perfmet
from atd_data_lake import config
from atd_data_lake.drivers.devices import gs_investigate
from atd_data_lake.util import date_util

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="gs_json_standard.py",
    appDescr="Performs JSON canonicalization for GRIDSMART data between the 'raw' and 'rawjson' Data Lake buckets")

class GSJSONStandardApp(etl_app.ETLApp):
    """
    Application functions and special behavior around GRIDSMART JSON canonicalization.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        self.forceUnitDate = None
        super().__init__("gs", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="raw",
                         purposeTgt="standardized",
                         perfmetStage="Standardize")
        self.prevDate = None
        self.unitDataProv = None
        self.prevUnitData = None
        self.siteFileCatElems = None
        self.siteFileCache = {}
        
    def _addCustomArgs(self, parser):
        """
        Adds custom unit date to accommodate addition of new GRIDSMART network
        """
        parser.add_argument("-U", "--unit_date", help="Force unit file date: YYYY-MM-DD format")

    def _ingestArgs(self, args):
        """
        Custom processing of force unit date
        """
        super()._ingestArgs(args)
        
        # Force unit date:
        if hasattr(args, "unit_date") and args.unit_date:
            self.forceUnitDate = date_util.parseDate(args.unit_date, dateOnly=True)
        else:
            self.forceUnitDate = None
    
    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for GRIDSMART:
        self.unitDataProv = config.createUnitDataAccessor(self.storageSrc)
        if self.forceUnitDate:
            self.unitDataProv.prepare(self.forceUnitDate)
        else:
            self.unitDataProv.prepare(self.startDate, self.endDate)
        
        # Prepare to get site files:
        self.siteFileCatElems = self.storageSrc.catalog.getSearchableQueryDict(self.storageSrc.repository,
                                                                               base=None, 
                                                                               ext="site.json",
                                                                               earlyDate=self.startDate,
                                                                               lateDate=self.endDate)
                
        # Configure the source and target repositories and start the compare loop:
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc, extFilter="zip"),
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=False)
        self.perfmet.writeSensorObs()
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Commit old sensor observations:
        if self.prevDate and item.identifier.date > self.prevDate:
            # Write out uncommitted sensor performance metric observations for the prior date:
            self.perfmet.writeSensorObs()
            
        # Get site file:
        siteFileCatElem, newSiteFlag = self.siteFileCatElems.getForPrevDate(item.identifier.base, item.identifier.date, forceValid=True)
        if not siteFileCatElem:
            print("ERROR: No site file is found for '%s' for date %s." % (item.identifier.base, str(item.identifier.date)))
            return 0        
        if not newSiteFlag:
            siteFile = self.siteFileCache[item.identifier.base]
        else:
            # Get site file from repository if needed:
            siteFile = json.loads(self.storageSrc.retrieveBuffer(siteFileCatElem["pointer"]))
            self.siteFileCache[item.identifier.base] = siteFile
        
        # Obtain unit data, and write it to the target repository if it's new:
        unitData = self.unitDataProv.retrieve(self.forceUnitDate if self.forceUnitDate else item.identifier.date)
        if unitData != self.prevUnitData:
            config.createUnitDataAccessor(self.storageTgt).store(unitData)
            
        print("%s: %s -> %s" % (item.label, self.storageSrc.repository, self.storageTgt.repository))
        worker = GSJSONStandard(item, siteFile, self.storageSrc, self.storageTgt, self.processingDate)
        if not worker.jsonize():
            return 0

        # Write the site file if it is a new one:
        if newSiteFlag:
            catalogElement = self.storageTgt.createCatalogElement(item.identifier.base, "site.json",
                                                                  siteFileCatElem["collection_date"], self.processingDate)
            self.storageTgt.writeJSON(siteFile, catalogElement)
            
        # Performance metrics logging:
        self.perfmet.recordCollect(item.identifier.date, representsDay=True)
        self.perfmet.recordSensorObs(item.identifier.base, "Vehicle Counts", perfmet.SensorObs(observation=worker.perfWork[0],
            expected=None, collectionDate=item.identifier.date, minTimestamp=worker.perfWork[1], maxTimestamp=worker.perfWork[2]))
        
        return 1

class GSJSONStandard:
    """
    Class standardizes GRIDMSMART directory data into JSON, with one file per GUID
    """
    def __init__(self, item, siteFile, storageSrc, storageTgt, processingDate):
        self.item = item
        self.siteFile = siteFile
        self.storageSrc = storageSrc
        self.storageTgt = storageTgt
        self.processingDate = processingDate

        self.apiVersion = None
        self.columns = None
        self.header = self.getHeader()
        self.perfWork = [0, None, None]
        
    @staticmethod
    def getAPIVersion(fileDict):
        csvPath0 = next(iter(fileDict.values()))
        apiVersion = int(pd.read_csv(csvPath0,
                                     header=None, usecols=[0],
                                     squeeze=True).unique()[0])
        return apiVersion

    def setDataColumns(self):
        if self.apiVersion == 8:
            self.columns = ["count_version", "site_version", "timestamp",
                            "utc_offset", "turn", "vehicle_length", "speed",
                            "light_state", "seconds_in_zone",
                            "vehicles_in_zone", "light_state_sec",
                            "sec_since_green", "zone_freeflow_speed",
                            "zone_freeflow_speed_cal"]
        elif self.apiVersion == 7:
            self.columns = ["count_version", "site_version", "timestamp",
                            "utc_offset", "turn", "vehicle_length", "speed",
                            "light_state", "seconds_in_zone",
                            "vehicles_in_zone", "confidence"]
        elif self.apiVersion == 4:
            self.columns = ["count_version", "site_version",  "timestamp",
                            "internal_veh_id", "internal_veh_type",
                            "vehicle_length", "speed", "turn", "allowable_turns",
                            "seconds_in_zone", "seconds_since_last_exit",
                            "queue_length", "light_state_on_exit",
                            "sec_since_green", "internal_frame_count", "day_night"]
        else:
            raise Exception("GRIDSMART counts file format %d is not supported." % self.apiVersion)

    def getHeader(self):
        header = {"data_type": "gridsmart",
                  "zip_name": self.item.label.split("/")[-1],
                  "origin_filename": None,
                  "target_filename": None,
                  "collection_date": str(self.item.identifier.date),
                  "processing_date": str(self.processingDate),
                  "version": self.apiVersion,
                  "guid": None}
        # Note: None values will be replaced in a per-GUID file basis
        return header

    def jsonize(self):
        # Read the .ZIP file and unpack here.
        filePath = self.storageSrc.retrieveFilePath(self.item.provItem.payload["pointer"])
        if not gs_investigate.investigate(filePath, lambda fileDict: self._jsonizeWork(fileDict)):
            print("File %s not processed." % filePath)
            return False
        
        # Clean up:
        os.remove(filePath)
        return True
    
    @staticmethod
    def _getTime(gsTimeString, tzInfo=None):
        """
        Parses the GRIDSMART-formatted time string and returns as UTC time, or given time zone.
        
        @param gsTimeString Time printed in format provided by GRIDSMART devices
        @param tzInfo Optional parenthesized time zone information-- e.g. "(GMT-06:00)"
        """
        if tzInfo:
            tzInfo = tzInfo.replace(":", "")
            return datetime.datetime.strptime(gsTimeString + " " + tzInfo, "%m/%d/%Y %I:%M:%S %p (%Z%z)")
        else:
            return pytz.utc.localize(datetime.datetime.strptime(gsTimeString, "%m/%d/%Y %I:%M:%S %p"))
        
    def _jsonizeWork(self, fileDict):
        n = len(fileDict)
        i = 0
        self.apiVersion = self.getAPIVersion(fileDict)
        self.setDataColumns()
        for key, value in fileDict.items():
            guid = key
            csvPath = value # Recall this is a temporary location from unzipped
            collDateStr = str(self.item.identifier.date) 
            targetFilename = self.item.identifier.base + '_' + guid + "_" + collDateStr.split()[0] + '.json'

            print(("Working on file {}").format(csvPath))
            # Initiate json object
            jsonData = {'header': self.header,
                        'data': None}
            # Add header information
            jsonData['header']['origin_filename'] = guid + '.csv'
            jsonData['header']['target_filename'] = targetFilename
            jsonData['header']['version'] = self.apiVersion
            jsonData['header']['guid'] = guid

            data = pd.read_csv(csvPath, header=None, names=self.columns)
            jsonData['data'] = data.apply(lambda x: x.to_dict(), axis=1).tolist()

            # Fix the time representation. First, find the time delta:
            errs = {}
            newData = []
            try:
                hostTimeUTC = self._getTime(self.siteFile["datetime"]["HostTimeUTC"])
                deviceTime = self._getTime(self.siteFile["datetime"]["DateTime"], self.siteFile["datetime"]["TimeZoneId"].split()[0])
                timeDelta = hostTimeUTC - deviceTime
                
                # At this point, collect an indication of whether this file accounts for some of the previous day, or some of the
                # next day.
                collDatetime = self.item.identifier.date.replace(hour=0, minute=0, second=0, microsecond=0)
                timestamp = None
                if self.apiVersion == 8 and jsonData['data']:
                    timestamp = datetime.datetime.strptime(collDateStr.split()[0] + " 000000", "%Y-%m-%d %H%M%S")
                    timestamp -= datetime.timedelta(minutes=jsonData['data'][0]['utc_offset'])
                    timestamp = pytz.utc.localize(timestamp)
                    timestamp = date_util.localize(timestamp + timeDelta)
                elif self.apiVersion == 7:
                    print("WARNING: 'timestamp_adj' processing not provided for API v7!")
                    # TODO: Figure out the date parsing needed for this.
                elif self.apiVersion == 4:
                    timestamp = datetime.datetime.strptime(collDateStr.split()[0] + " 000000", "%Y-%m-%d %H%M%S")
                    timestamp = pytz.utc.localize(timestamp)
                    timestamp = date_util.localize(timestamp + timeDelta)
                if timestamp:
                    if timestamp < collDatetime:
                        jsonData['header']['day_covered'] = -1
                    elif timestamp == collDatetime:
                        jsonData['header']['day_covered'] = 0
                    else:
                        jsonData['header']['day_covered'] = 1
                
                # Add in "timestamp_adj" for each data item:
                for item in jsonData['data']:
                    try:
                        if self.apiVersion == 8:
                            # TODO: The UTC Offset doesn't seem to reflect DST. Should we ignore it and blindly localize instead?
                            #       We can figure this out by seeing what the latest count is on a live download of the current day.
                            timestamp = datetime.datetime.strptime(collDateStr.split()[0] + " " \
                                + ("%06d" % int(float(item['timestamp']))) + "." + str(round((item['timestamp'] % 1) * 10) * 100000),
                                "%Y-%m-%d %H%M%S.%f")
                            timestamp -= datetime.timedelta(minutes=item['utc_offset'])
                            timestamp = pytz.utc.localize(timestamp)
                            item['timestamp_adj'] = str(date_util.localize(timestamp + timeDelta))
                        elif self.apiVersion == 7:
                            print("WARNING: 'timestamp_adj' processing not provided for API v7!")
                            # TODO: Figure out the date parsing needed for this.
                        elif self.apiVersion == 4:
                            timestamp = datetime.datetime.strptime(item['timestamp'], "%Y%m%dT%H%M%S" + (".%f" if "." in item['timestamp'] else ""))
                            timestamp = pytz.utc.localize(timestamp)
                            item['timestamp_adj'] = str(date_util.localize(timestamp + timeDelta))
                            
                            item['count_version'] = int(item['count_version'])
                        if timestamp:
                            # Performance metrics:
                            if not self.perfWork[1]:
                                self.perfWork = [0, timestamp, timestamp]
                            self.perfWork[0] += 1
                            if timestamp < self.perfWork[1]:
                                self.perfWork[1] = timestamp
                            if timestamp > self.perfWork[2]:
                                self.perfWork[2] = timestamp
                        newData.append(item)
                    except ValueError as exc:
                        err = "WARNING: Value parsing error: " + str(exc)
                        if err not in errs:
                            errs[err] = 0
                        errs[err] += 1
                jsonData['data'] = newData
                for err in errs:
                    print(err + " (" + str(errs[err]) + ")")
            except KeyError:
                print("WARNING: Time representation processing has malfunctioned. Correct time key may not be present in site file.")
            except ValueError as exc:
                print("WARNING: Time representation processing has malfunctioned. Value parsing error:")
                print(exc)
            
            # Write to storage object:
            catalogElement = self.storageTgt.createCatalogElement(self.item.identifier.base, guid + ".json", 
                self.item.identifier.date, processingDate=self.processingDate)
            self.storageTgt.writeJSON(jsonData, catalogElement, cacheCatalogFlag=True)
            
            i += 1
            print("JSON standardization saved as {}".format(targetFilename))
            print("File {} out of {} done!".format(i, n))
            
def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = GSJSONStandardApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
