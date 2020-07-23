"""
ATD Data Lake 'ready' bucket for GRIDSMART

@author Kenneth Perrine, Nadia Florez
"""
import datetime, collections, difflib, json, traceback

import arrow

from support import etl_app, last_update
import config
from config import config_app
from util import gps_h

# This sets up application information:
APP_DESCRIPTION = etl_app.AppDescription(
    appName="gs_ready.py",
    appDescr="Performs JSON enrichment for GRIDSMART data between the 'rawjson' and 'ready' Data Lake buckets")

"The minimum allowable ratio for fuzzy string matching"
MIN_MATCH_RATIO = 0.7

"Maximum distance (in feet) for nearest GRIDSMART device match when naming can't be found"
MAX_DIST = 300

class GSReadyApp(etl_app.ETLApp):
    """
    Application functions and special behavior around GRIDSMART ingestion.
    """
    def __init__(self, args):
        """
        Initializes application-specific variables
        """
        super().__init__("gs", APP_DESCRIPTION,
                         args=args,
                         purposeSrc="rawjson",
                         purposeTgt="ready",
                         needsTempDir=True,
                         perfmetStage="Ready")
        self.unitDataProv = None
        self.prevUnitData = None
        self.siteFileCatElems = None
        self.siteFileCache = {}
        self.bases = set()
        self.curDate = None
    
    def _addCustomArgs(self, parser):
        """
        Override this and call parser.add_argument() to add custom command-line arguments.
        """
        parser.add_argument("-p", "--ignore_prev", action="store_true", default=False, help="ignore previous day for 24-hour completion")
        parser.add_argument("-n", "--ignore_next", action="store_true", default=False, help="ignore next day for 24-hour completion")

    def etlActivity(self):
        """
        This performs the main ETL processing.
        
        @return count: A general number of records processed
        """
        # First, get the unit data for GRIDSMART:
        self.unitDataProv = config.createUnitDataAccessor(self.storageSrc).prepare(self.startDate, self.endDate)

        # Prepare to get site files:
        self.siteFileCatElems = self.storageSrc.catalog.getSearchableQueryDict(self.storageSrc.repository, "site.json",
                                                                               earlyDate=self.startDate,
                                                                               lateDate=self.endDate)
        
        # Configure the source and target repositories and start the compare loop:
        self.bases.clear()
        self.curDate = None
        count = self.doCompareLoop(last_update.LastUpdStorageCatProv(self.storageSrc, extFilter="%%.json"),
                                   last_update.LastUpdStorageCatProv(self.storageTgt),
                                   baseExtKey=False)
        count += self._processDay(self.curDate)
        print("Records processed: %d" % count)
        return count    

    def innerLoopActivity(self, item):
        """
        This is where the actual ETL activity is called for the given compare item.
        """
        # Filter out unit and site files here because we handle them specially later.
        if item.identifier.ext == "site.json" or item.identifier.ext == "unit_data.json":
            return 0
        
        # Process if we have enough dates:
        count = 0
        if not self.curDate:
            self.curDate = self.item.identifier.date
        if self.item.identifier.date > self.curDate:
            count = self._processDay(self.curDate)
            self.item.identifier.date = self.curDate
        
        # Pre-load catalog entries to allow processing of all GUIDs for all intersections:
        if item.identifier.base not in self.bases:
            self.bases.add(item.identifier.base)
        
        return count
        
    def _processDay(self, date):
        """
        The code is set up to collect all catalog entries for each day. Here, we analyze the alignment of logged
        entries with the actual time (e.g. there's clock drift or bad time zones), retrieve the records that we
        need, and then create a new time-aligned, completed JSON count for each intersection. 
        """
        count = 0
        
        # Obtain unit data:
        unitData = self.unitDataProv.retrieve(date)
        
        # Iterate through each intersection:
        sortedBases = sorted(self.bases)
        for base in sortedBases:
            print("== " + sortedBases + ": " + date.strfdate("%Y-%m-%d") + " ==")
            
            # Step 1: Get site file:
            siteFileCatElem, newSiteFlag = self.siteFileCatElems.getForPrevDate(base, date, forceValid=True)
            if not siteFileCatElem:
                print("ERROR: No site file is found for '%s' for date %s." % (base, str(date)))
                continue
            if not newSiteFlag:
                siteFile = self.siteFileCache[base]
            else:
                # Get site file from repository if needed:
                siteFile = json.loads(self.storageSrc.retrieveBuffer(siteFileCatElem["path"]))
                self.siteFileCache[base] = siteFile
            
            # Step 2: Resolve the base to the units file:
            # Basically we need to take site.Location.Street1 and .Street2 and positively identify the
            # corresponding record in unit_data.devices[].primary_st and .cross_st.
            
            # Stage 0: First try to see if we are explicitly called out in config.KNACK_LOOKUPS.
            matchedDevice = None
            reverseFlag = False
            testStr = siteFile["site"]["Location"]["Street1"].strip() + "_" + siteFile["site"]["Location"]["Street2"].strip()
            if testStr in config_app.KNACK_LOOKUPS:
                for deviceItem in unitData["devices"]:
                    if deviceItem["atd_location_id"] == config_app.KNACK_LOOKUPS[testStr]:
                        matchedDevice = deviceItem
                        break
                else:
                    print("WARNING: The respective ID '%s' is not found for the 'KNACK_LOOKUPS' entry for '%s'." % (config_app.KNACK_LOOKUPS[testStr], testStr))
            else:
                # Stage 1: Do fuzzy matching to match respective Knack entry:
                matchedDevice = None
                street1Sub = siteFile["site"]["Location"]["Street1"].strip()
                street2Sub = siteFile["site"]["Location"]["Street2"].strip()
                testStr = (street1Sub + " " + street2Sub).lower()
                
                compareList = []
                for deviceItem in unitData["devices"]:
                    if str(deviceItem["primary_st"]) == "nan" or str(deviceItem["cross_st"]) == "nan":
                        continue
                    compareList.append(_CompareEntry((deviceItem["primary_st"].strip() + " " + deviceItem["cross_st"].strip()).lower(), False, deviceItem))
                    compareList.append(_CompareEntry((deviceItem["cross_st"].strip() + " " + deviceItem["primary_st"].strip()).lower(), True, deviceItem))
                winningEntry, maxRatio = _findFuzzyWinner(compareList, testStr)
                if maxRatio < MIN_MATCH_RATIO:
                    # Stage 2: Try fuzzy matching with "STREET_SYNONYMS" string substitutions if they're available.
                    if street1Sub in config_app.STREET_SYNONYMS:
                        street1Sub = config_app.STREET_SYNONYMS[street1Sub]
                    if street2Sub in config_app.STREET_SYNONYMS:
                        street2Sub = config_app.STREET_SYNONYMS[street2Sub]
                    testStr2 = (street1Sub + " " + street2Sub).lower()
                    if testStr != testStr2:
                        winningEntry, maxRatio = _findFuzzyWinner(compareList, testStr2)
                    if maxRatio < MIN_MATCH_RATIO:
                        # Stage 3: Try matching IP addresses.
                        print("WARNING: No unit_data device could be discerned by name for GRIDSMART device '%s'." % base)
                        if "device_net_addr" in siteFile["header"]:
                            netAddr = siteFile["header"]["device_net_addr"]
                            for deviceItem in unitData["devices"]:
                                if deviceItem["device_ip"] == netAddr:
                                    print("INFO: Matched IP address %s: '%s/%s'." % (netAddr, deviceItem["primary_st"], deviceItem["cross_st"]))
                                    matchedDevice = deviceItem
                                    break
                            else:                        
                                # Stage 4: Try GPS coordinate matching.
                                print("WARNING: Could not match by IP address.")
                                minDistance = None
                                minDistDevice = None
                                for deviceItem in unitData["devices"]:
                                    dist = gps_h.gps2feet(float(siteFile["site"]["Location"]["Latitude"]), float(siteFile["site"]["Location"]["Longitude"]),
                                                      float(deviceItem["lat"]), float(deviceItem["lon"]))
                                    if minDistance is None or minDistance > dist:
                                        minDistance = dist
                                        minDistDevice = deviceItem
                                        
                                if minDistance < MAX_DIST:
                                    print("Matched at %d feet to nearest GPS coords: '%s/%s'" % (minDistance, deviceItem["primary_st"], deviceItem["cross_st"]))
                                    matchedDevice = minDistDevice
                                else:
                                    print("WARNING: Also could not match to nearest GPS coordinates.")
                    else:
                        print("INFO: Matched on substituted string key: '%s'" % testStr2)
                if maxRatio >= MIN_MATCH_RATIO:
                    matchedDevice = winningEntry.item
                    reverseFlag = winningEntry.reverseFlag
                    
                # Caution: This mutates the device file cache.
                if matchedDevice:
                    matchedDevice["reversed"] = reverseFlag

            # Step 3: Gather counts files:
            # Iterate through the GUID/approach files:
            countsReceiver = []
            repHeader = None
            dayDirErr = 0
            for cameraDeviceItem in siteFile["site"]["CameraDevices"]:
                print("Camera MAC address: %s" % cameraDeviceItem["Fisheye"]["MACAddress"])
                if not cameraDeviceItem["Fisheye"]["IsConfigured"]:
                    print("Ignoring because it isn't configured.")
                    continue
                for zoneMaskItem in cameraDeviceItem["Fisheye"]["CameraMasks"]["ZoneMasks"]:
                    if "Vehicle" not in zoneMaskItem:
                        continue
                    if not zoneMaskItem["Vehicle"]["IncludeInData"]:
                        continue
                    ident = zoneMaskItem["Vehicle"]["Id"]
                    guid = ident[0:8] + "-" + ident[8:12] + "-" + ident[12:16] + "-" + ident[16:20] + "-" + ident[20:]

                    # First, get the current day's file:
                    curDayCounts = getCountsFile(date, base, guid, self.storageSrc)
                    if curDayCounts:
                        try:
                            fillDayRecords(date, curDayCounts, ident, countsReceiver)
                        except KeyError:
                            traceback.print_exc()
                            continue
                        
                        # Next, figure out which supplemental day's file we need in order to get the full picture:
                        auxDate = None
                        if "day_covered" in curDayCounts["header"]:
                            if curDayCounts["header"]["day_covered"] == 1:
                                auxDate = date - datetime.timedelta(days=1) # We have to get some of yesterday.
                            elif curDayCounts["header"]["day_covered"] == -1:
                                auxDate = date + datetime.timedelta(days=1) # We have to get some of tomorrow.
                        else:
                            print("WARNING: 'day_covered' is missing from header; data from adjacent day may be missing.")
                        
                        auxDayCounts = None
                        header = curDayCounts["header"]
                        if auxDate:
                            del curDayCounts # Memory management
                            auxDayCounts = getCountsFile(auxDate, base, guid, self.storageSrc)
                            if auxDayCounts:
                                try:
                                    fillDayRecords(date, auxDayCounts, ident, countsReceiver)
                                except KeyError:
                                    traceback.print_exc()
                                    continue
                            else:
                                print("WARNING: GUID %s is not found for the auxiliary (%s) day file." % (str(auxDate), guid))
                                dayDirErr = header["day_covered"]
                        
                        # Store a representative header:
                        if not repHeader:
                            repHeader = curDayCounts["header"] 
                    else:
                        print("WARNING: GUID %s is not found for current day file." % guid)
                        
            # Completion checking:
            if dayDirErr == 1 and not self.args.ignore_prev:
                print("ERROR: No records from previous day were found, aborting. This can be ignored if the -p flag is specified.")
                continue
            elif dayDirErr == -1 and not self.args.ignore_next:
                print("ERROR: No records from next day were found, aborting. This can be ignored if the -n flag is specified.")
                continue
                
            if not countsReceiver:
                print("ERROR: No counts were found.")
            else:
                # Step 4: Write out compiled counts:
                countsReceiver.sort(key=lambda c: c["timestamp_adj"])
                
                header = {"data_type": "gridsmart",
                          "zip_name": repHeader["zip_name"],
                          "collection_date": repHeader["collection_date"],
                          "processing_date": str(self.processingDate),
                          "version": repHeader["version"]}
                
                newFileContents = {"header": header,
                                   "counts": countsReceiver,
                                   "site": siteFile,
                                   "device": matchedDevice if matchedDevice else []}
                
                # TODO: Continue to see out how to positively resolve NORTHBOUND, EASTBOUND, etc. to street geometry.
                catalogElem = self.storageTgt.createCatalogElement(base, "counts.json", date, self.processingDate)
                self.storageTgt.writeJSON(newFileContents, catalogElem, cacheCatalogFlag=True)

                # Performance metrics:
                self.perfmet.recordCollect(date, representsDay=True)

                # Increment count:
                count += 1
                
        self.bases.clear()
        return count
    
def getCountsFile(base, date, guid, storage):
    """
    Using a base (street intersection name), attempts to retrieve from storage the file that corresponds with
    the given GUID for the given date. Returns path to the file, or None if it doesn't exist.   
    """
    catalogElement = storage.catalog.querySingle(storage.repository, base, guid + ".json", date)
    if not catalogElement:
        return None
    return storage.retrieveJSON(catalogElement["path"])

def fillDayRecords(ourDate, countsFileData, ident, receiver):
    "Caution: this mutates countsFileData."
    
    ourDateMax = ourDate + datetime.timedelta(days=1)
    for item in countsFileData["data"]:
        timestamp = arrow.get(item["timestamp_adj"]).datetime
        if timestamp >= ourDate and timestamp < ourDateMax:
            # This record falls within range:
            item["zone"] = ident
            receiver.append(item)
    
_CompareEntry = collections.namedtuple("_CompareEntry", "compareStr reverseFlag item")

def _findFuzzyWinner(compareEntries, testStr):
    "Performs fuzzy matching on the compareEntries list and returns the winning item."
    
    matchedEntry = None
    maxRatio = 0
    for entry in compareEntries:
        ratio = difflib.SequenceMatcher(None, testStr, entry.compareStr).ratio()
        if ratio > maxRatio:
            matchedEntry = entry
            maxRatio = ratio
    return matchedEntry, maxRatio

def main(args=None):
    """
    Main entry point. Allows for dictionary to bypass default command-line processing.
    """
    curApp = GSReadyApp(args)
    return curApp.doMainLoop()

if __name__ == "__main__":
    """
    Entry-point when run from the command-line
    """
    main()
