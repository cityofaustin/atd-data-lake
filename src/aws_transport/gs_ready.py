'''
CoA Data Lake 'ready' bucket for GRIDSMART
Author: Kenneth Perrine, Nadia Florez
'''

import json
import os
import collections
import tempfile
import shutil
import datetime
import difflib
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import traceback

from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import last_upd_cat, unit_data, gps_h, config
from util import date_util

PROGRAM_DESC = "Performs JSON enrichment for GRIDSMART data between the 'rawjson' and 'ready' Data Lake buckets"

"Number of months to go back for filing records"
DATE_EARLIEST = 12

"S3 bucket as source"
SRC_BUCKET = config.composeBucket("rawjson")

"S3 bucket to target"
TGT_BUCKET = config.composeBucket("ready")

"The minimum allowable ratio for fuzzy string matching"
MIN_MATCH_RATIO = 0.7

"Maximum distance (in feet) for nearest GRIDSMART device match when naming can't be found"
MAX_DIST = 300
    
def getCountsFile(ourDate, baseName, guid, s3, catalog):
    command = {"select": "id_base,id_ext,collection_date,pointer,metadata",
               "repository": "eq.%s" % "rawjson",
               "data_source": "eq.%s" % "gs",
               "id_base": "eq.%s" % baseName,
               "id_ext": "eq.%s.json" % guid,
               "collection_date": "eq.%s" % str(ourDate),
               "limit": 1}
    catResults = catalog.select(params=command)
    if not catResults:
        # No record found.
        return None
    dataPointer = catResults[0]["pointer"]
    contentObj = s3.Object(SRC_BUCKET, dataPointer)
    print("Reading: %s" % dataPointer)
    fileContent = contentObj.get()['Body'].read().decode('utf-8')
    countsFileData = json.loads(fileContent)
    return countsFileData

def fillDayRecords(ourDate, countsFileData, ident, receiver):
    "Caution: this mutates countsFileData."
    
    ourDateMax = ourDate + datetime.timedelta(days=1)
    for item in countsFileData["data"]:
        timestamp = arrow.get(item["timestamp_adj"]).datetime
        if timestamp >= ourDate and timestamp < ourDateMax:
            # This record falls within range:
            item["zone"] = ident
            receiver.append(item)

def set_S3_pointer(filename, date, data_source='gs'):

    year = str(date.year)
    month = str(date.month)
    day = str(date.day)

    s_year = year
    s_month = month if len(month) == 2 else month.zfill(2)
    s_day = day if len(day) == 2 else day.zfill(2)

    return "{year}/{month}/{day}/{data_source}/{file}".format(year=s_year,
                                                            month=s_month,
                                                            day=s_day,
                                                            data_source=data_source,
                                                            file=filename)
    
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

def main():
    "Main entry-point that takes --last_run_date parameter"

    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-r", "--last_run_date", help="last run date, in YYYY-MM-DD format with optional time zone offset")
    parser.add_argument("-m", "--months_old", help="process no more than this number of months old, or provide YYYY-MM-DD for absolute date")
    parser.add_argument("-M", "--missing", action="store_true", default=False, help="check for missing entries after the earliest processing date")
    parser.add_argument("-s", "--same_day", action="store_true", default=False, help="retrieves and processes files that are the same day as collection")
    parser.add_argument("-p", "--ignore_prev", action="store_true", default=False, help="ignore previous day for 24-hour completion")
    parser.add_argument("-n", "--ignore_next", action="store_true", default=False, help="ignore next day for 24-hour completion")
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    date_util.setLocalTimezone(config.TIMEZONE)
    lastRunDate = date_util.parseDate(args.last_run_date, dateOnly=True)
    print("gs_ready: Last run date: %s" % str(lastRunDate))

    if args.months_old:
        try:
            monthsOld = int(args.months_old)
        except ValueError:
            monthsOld = date_util.parseDate(args.months_old, dateOnly=True)
    else:
        monthsOld = DATE_EARLIEST

    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    s3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    tempDir = tempfile.mkdtemp()
    print("Created holding place: %s" % tempDir)
    
    # Set up object for retrieving the Unit Data files:
    unitData = unit_data.UnitData(catalog, s3, SRC_BUCKET, "rawjson", "gs")

    # Gather records of prior activity from catalog:
    print("Pre-selecting...")
    lastUpdateWorker = last_upd_cat.LastUpdateCat("rawjson", "ready", "gs", monthsOld)
    bases = {}
    for record in lastUpdateWorker.getToUpdate(lastRunDate, sameDay=args.same_day, detectMissing=args.missing):
        if record.identifier[1] == "unit_data.json" or record.identifier[1] == "site.json":
            # TODO: Figure out a better way to deal with avoiding unit_data in LastUpdateCat work.
            continue
        rootName = record.identifier[0]
        if rootName not in bases:
            bases[rootName] = {} 
        bases[rootName][record.fileDate] = "/".join(record.s3Path.split("/")[:-1])

    count = 0
    for base in sorted(bases.keys()): # TODO: We should probably go from earliest date to latest date rather than per detector. Easier to recover when there's an error.
        prevLastDate = None
        siteFileData = None
        unitFileCache = {}
        print("== " + base + " ==")
        dateList = sorted(bases[base])
        
        cachedDate = None
        cachedDayFiles = None
        for ourDate in dateList:
            # TODO: Put a lot of this functionality into utility functions!
            print("- " + str(ourDate) + " -")
            
            # STEP 1: Get the unit file:
            collectDate = ourDate
            command = {"select": "collection_date,pointer,metadata",
                       "repository": "eq.%s" % "rawjson",
                       "data_source": "eq.%s" % "gs",
                       "id_base": "eq.%s" % config.UNIT_LOCATION,
                       "id_ext": "eq.unit_data.json",
                       "collection_date": "gte.%s" % str(collectDate),
                       "order": "collection_date",
                       "limit": 1}
            catResults = catalog.select(params=command)
            if not catResults:
                # No record found.
                # TODO: We could look for the most recent data file up to the date.
                raise Exception("No applicable unit file found for Date: %s" % str(collectDate))
            lastDate = date_util.localize(arrow.get(catResults[0]["collection_date"]).datetime)
            dataPointer = catResults[0]["pointer"]
            
            if lastDate not in unitFileCache:
                contentObj = s3.Object(SRC_BUCKET, dataPointer)
                fileContent = contentObj.get()['Body'].read().decode('utf-8')
                unitFileData = json.loads(fileContent)
                unitFileCache[lastDate] = unitFileData
            else:
                unitFileData = unitFileCache[lastDate]
            
            # STEP 2: Get the site file:
            collectDate = ourDate
            command = {"select": "collection_date,pointer,metadata",
                       "repository": "eq.%s" % "rawjson",
                       "data_source": "eq.%s" % "gs",
                       "id_base": "eq.%s" % base,
                       "id_ext": "eq.site.json",
                       "collection_date": "gte.%s" % str(collectDate),
                       "order": "collection_date",
                       "limit": 1}
            catResults = catalog.select(params=command)
            if not catResults:
                # No record found.
                # TODO: We could look for the most recent data file up to the date.
                raise Exception("No applicable site file found for file base: %s; Date: %s" %
                                (base, str(collectDate)))
            lastDate = date_util.localize(arrow.get(catResults[0]["collection_date"]).datetime)
            dataPointer = catResults[0]["pointer"]
            
            if prevLastDate != lastDate:
                contentObj = s3.Object(SRC_BUCKET, dataPointer)
                fileContent = contentObj.get()['Body'].read().decode('utf-8')
                siteFileData = json.loads(fileContent)
                prevLastDate = lastDate
                
            # STEP 3: Resolve the base to the units file:
            # Basically we need to take site.Location.Street1 and .Street2 and positively identify the
            # corresponding record in unit_data.devices[].primary_st and .cross_st.
            
            # Stage 0: First try to see if we are explicitly called out in config.KNACK_LOOKUPS.
            matchedDevice = None
            reverseFlag = False
            testStr = siteFileData["site"]["Location"]["Street1"].strip() + "_" + siteFileData["site"]["Location"]["Street2"].strip()
            if testStr in config.KNACK_LOOKUPS:
                for deviceItem in unitFileData["devices"]:
                    if deviceItem["atd_location_id"] == config.KNACK_LOOKUPS[testStr]:
                        matchedDevice = deviceItem
                        break
                else:
                    print("WARNING: The respective ID '%s' is not found for the 'KNACK_LOOKUPS' entry for '%s'." % (config.KNACK_LOOKUPS[testStr], testStr))
            else:
                # Stage 1: Do fuzzy matching to match respective Knack entry:
                matchedDevice = None
                street1Sub = siteFileData["site"]["Location"]["Street1"].strip()
                street2Sub = siteFileData["site"]["Location"]["Street2"].strip()
                testStr = (street1Sub + " " + street2Sub).lower()
                
                compareList = []
                for deviceItem in unitFileData["devices"]:
                    if str(deviceItem["primary_st"]) == "nan" or str(deviceItem["cross_st"]) == "nan":
                        continue
                    compareList.append(_CompareEntry((deviceItem["primary_st"].strip() + " " + deviceItem["cross_st"].strip()).lower(), False, deviceItem))
                    compareList.append(_CompareEntry((deviceItem["cross_st"].strip() + " " + deviceItem["primary_st"].strip()).lower(), True, deviceItem))
                winningEntry, maxRatio = _findFuzzyWinner(compareList, testStr)
                if maxRatio < MIN_MATCH_RATIO:
                    # Stage 2: Try fuzzy matching with "STREET_SYNONYMS" string substitutions if they're available.
                    if street1Sub in config.STREET_SYNONYMS:
                        street1Sub = config.STREET_SYNONYMS[street1Sub]
                    if street2Sub in config.STREET_SYNONYMS:
                        street2Sub = config.STREET_SYNONYMS[street2Sub]
                    testStr2 = (street1Sub + " " + street2Sub).lower()
                    if testStr != testStr2:
                        winningEntry, maxRatio = _findFuzzyWinner(compareList, testStr2)
                    if maxRatio < MIN_MATCH_RATIO:
                        # Stage 3: Try matching IP addresses.
                        print("WARNING: No unit_data device could be discerned by name for GRIDSMART device '%s'." % base)
                        if "device_net_addr" in siteFileData["header"]:
                            netAddr = siteFileData["header"]["device_net_addr"]
                            for deviceItem in unitFileData["devices"]:
                                if deviceItem["device_ip"] == netAddr:
                                    print("INFO: Matched IP address %s: '%s/%s'." % (netAddr, deviceItem["primary_st"], deviceItem["cross_st"]))
                                    matchedDevice = deviceItem
                                    break
                            else:                        
                                # Stage 4: Try GPS coordinate matching.
                                print("WARNING: Could not match by IP address.")
                                minDistance = None
                                minDistDevice = None
                                for deviceItem in unitFileData["devices"]:
                                    dist = gps_h.gps2feet(float(siteFileData["site"]["Location"]["Latitude"]), float(siteFileData["site"]["Location"]["Longitude"]),
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

            # STEP 4: Gather counts files:
            # Iterate through the GUID/approach files:
            countsReceiver = []
            repHeader = None
            newCachedDate = None
            newCachedFiles = {}
            dayDirErr = 0
            for cameraDeviceItem in siteFileData["site"]["CameraDevices"]:
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
                    curDayCounts = None
                    if cachedDate == ourDate and ident in cachedDayFiles:
                        curDayCounts = cachedDayFiles[ident]
                        print("Reading cached for GUID %s on %s" % (ident, str(cachedDate)))
                        del cachedDayFiles[ident]
                    else:
                        curDayCounts = getCountsFile(ourDate, base, guid, s3, catalog)
                    if curDayCounts:
                        try:
                            fillDayRecords(ourDate, curDayCounts, ident, countsReceiver)
                        except KeyError:
                            traceback.print_exc()
                            continue
                        
                        # Next, figure out which supplemental day's file we need in order to get the full picture:
                        auxDate = None
                        if "day_covered" in curDayCounts["header"]:
                            if curDayCounts["header"]["day_covered"] == 1:
                                auxDate = ourDate - datetime.timedelta(days=1) # We have to get some of yesterday.
                            elif curDayCounts["header"]["day_covered"] == -1:
                                auxDate = ourDate + datetime.timedelta(days=1) # We have to get some of tomorrow.
                        else:
                            print("WARNING: 'day_covered' is missing from header; data from adjacent day may be missing.")
                            
                        auxDayCounts = None
                        if auxDate:
                            if cachedDate == auxDate and ident in cachedDayFiles:
                                auxDayCounts = cachedDayFiles[ident]
                                print("Reading cached for GUID %s on %s" % (ident, str(cachedDate)))
                                del cachedDayFiles[ident]
                            else:
                                auxDayCounts = getCountsFile(auxDate, base, guid, s3, catalog)
                            if auxDayCounts:
                                try:
                                    fillDayRecords(ourDate, auxDayCounts, ident, countsReceiver)
                                except KeyError:
                                    traceback.print_exc()
                                    continue
                            else:
                                print("WARNING: GUID %s is not found for the auxiliary (%s) day file." % (str(auxDate), guid))
                                dayDirErr = curDayCounts["header"]["day_covered"]
                        
                        # Get the cache staged properly:
                        if "day_covered" in curDayCounts["header"]:
                            if curDayCounts["header"]["day_covered"] == 1:
                                if newCachedDate is not None and newCachedDate != ourDate:
                                    print("ERROR: Cached date shift is inconsistent among GUIDs (caching current date)")
                                else:
                                    newCachedDate = ourDate
                                    newCachedFiles[ident] = curDayCounts
                            elif curDayCounts["header"]["day_covered"] == -1 and auxDayCounts:
                                if newCachedDate is not None and newCachedDate != auxDate:
                                    print("ERROR: Cached date shift is inconsistent among GUIDs (caching tomorrow's date)")
                                else:
                                    newCachedDate = auxDate
                                    newCachedFiles[ident] = auxDayCounts
                                
                        # Store a representative header:
                        if not repHeader:
                            repHeader = curDayCounts["header"] 
                    else:
                        print("WARNING: GUID %s is not found for current day file." % guid)
                        
            # Commit the cache:
            cachedDate = newCachedDate
            cachedDayFiles = newCachedFiles

            # Completion checking:
            if dayDirErr == 1 and not args.ignore_prev:
                print("ERROR: No records from previous day were found, aborting. This can be ignored if the -p flag is specified.")
                continue
            elif dayDirErr == -1 and not args.ignore_next:
                print("ERROR: No records from next day were found, aborting. This can be ignored if the -n flag is specified.")
                continue
                
            if not countsReceiver:
                print("ERROR: No counts were found.")
            else:
                # STEP 5: Write out compiled counts:
                countsReceiver.sort(key=lambda c: c["timestamp_adj"])
                
                header = {"data_type": "gridsmart",
                          "zip_name": repHeader["zip_name"],
                          "collection_date": repHeader["collection_date"],
                          "processing_date": str(date_util.localize(arrow.now().datetime)),
                          "version": repHeader["version"]}
                
                newFileContents = {"header": header,
                                   "counts": countsReceiver,
                                   "site": siteFileData,
                                   "device": matchedDevice if matchedDevice else []}
                
                # TODO: Continue to see out how to positively resolve NORTHBOUND, EASTBOUND, etc. to street geometry.
                targetBaseFile = base + "_counts_" + ourDate.strftime("%Y-%m-%d") 
                targetPath = set_S3_pointer(targetBaseFile + ".json", ourDate)
                
                # Write contents to S3:
                print("%s: %s -> %s" % (targetPath, SRC_BUCKET, TGT_BUCKET))
                fullPathW = os.path.join(tempDir, targetBaseFile + ".json")
                with open(fullPathW, 'w') as gsJSONFile:
                    json.dump(newFileContents, gsJSONFile)
        
                with open(fullPathW, 'rb') as gsJSONFile:
                    s3Object = s3.Object(TGT_BUCKET, targetPath)
                    s3Object.put(Body=gsJSONFile)
                    
                # Clean up:
                os.remove(fullPathW)

                # Update the catalog:
                metadata = {"repository": 'ready', "data_source": 'gs',
                            "id_base": base, "id_ext": "counts.json",
                            "pointer": targetPath,
                            "collection_date": header["collection_date"],
                            "processing_date": header["processing_date"], "metadata": {}}
                catalog.upsert(metadata)
                
                # Increment count:
                count += 1
                
    # Clean up the temporary output directory:
    shutil.rmtree(tempDir)

    print("Records processed: %d" % count)
    return count

if __name__ == "__main__":
    main()
