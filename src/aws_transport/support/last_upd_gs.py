"""
last_upd_gs.py: Assists with identifying list of last-updated GRIDSMART counts given catalog contents
and last-update parameter. Preliminary code until we figure out how to do this better.

Kenneth Perrine - 2019-02-08
"""

import numbers

from pypgrest import Postgrest
import arrow

from aws_transport.support import gs_knack_devices, config
from collecting.gs import log_reader
from util import date_util

def getDevicesLogreaders(devFilter=".*"):
    """
    Attempts to retrieve all devices and log readers using the gs_intersections table. Devices that can't be contacted
    are not added to the list, and aren't addressed in the LastUpdateGS material below.
    
    @return dict that's suitable for passing into LastUpdateGS.__init__() logReaders field
    """
    # Get the devices:
    gsWorker = gs_knack_devices.GSKnackDevices()
    devices = gsWorker.retrieveDevices(devFilter)
    
    # Get counts availability for all of these devices:
    ret = {}
    count = 0
    errs = 0
    print("== Collecting device availability ==")
    for index, device in enumerate(devices):
        print("Device: %d: %s_%s... " % (index, device.street1, device.street2), end='')
        try:
            logReader = log_reader.LogReader(device)
            ret[device] = logReader
            count += 1
            print("OK")
        except Exception as exc:
            print("ERROR: A problem was encountered in accessing.") 
            print(exc)
            errs += 1
    print("Result: Sucesses: %d; Failures: %d" % (count, errs))
    return ret, gsWorker.getAllFiles(), gsWorker.getKnackJSON()

class _GetToUpdateRet:
    """
    Return object for LastUpdateGS.getToUpdate()
    """
    def __init__(self, device, logReader, fileDate, missingFlag):
        """
        @param device The GRIDSMART device, represented as collecting.gs.device.Device. Note that this has no date!
        @param logReader The GRIDSMART log reader, represented as collecting.gs.log_reader.LogReader, used to get counts file:
                (that's through the getCountsFile() method)
        @param fileDate A datetime object that signifies the date of the record.
        @param missingFlag Signifies if this file had been detected as missing, preceding the lastRunDate.
        """
        self.device = device
        self.logReader = logReader
        self.fileDate = fileDate
        self.missingFlag = missingFlag

    def __str__(self):
        "Returns string representation."
        return "device: '%s'; fileDate: %s; missingFlag: %s" % ((self.device.street1 + "_" + self.device.street2), \
                                                                str(self.fileDate), str(self.missingFlag))
        
class LastUpdateGS:
    """
    Contains methods for creating the list of files to deal with.
    """
    def __init__(self, logReaders, tgtRepo, datatype, dateEarliest=None):
        """
        @param logReaders A dict of all GRIDSMART devices and log readers, represented as collecting.gs.device.Device -> collecting.gs.log_reader.LogReader
        @param tgtRepo The code for the targeted repository, for getting date information from the catalog.
        @param dataType The datatype as recorded in the catalog.
        @param dateEarliest The earliest date to treat as the minimum date to process. None for no earliest, datetime obj., or number for years.
        """
        self.logReaders = logReaders
        self.tgtRepo = tgtRepo
        self.datatype = datatype
                
        # Find the earliest date. If it's a number, then it's number of years.
        if isinstance(dateEarliest, numbers.Number):
            dateEarliest = date_util.localize(arrow.now()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .shift(months=-dateEarliest).datetime)
        self.dateEarliest = dateEarliest

    def getToUpdate(self, lastRunDate, sameDay=False, detectMissing=True):
        """
        Returns a list of all devices and dates that are needing to be selected and updated, given the lastRunDate,
        catalog, and earliest processing date. These are returned as a list of _GetToUpdateRet objects.
        
        @param lastRunDate datetime object that signifies the last run time.
        @param sameDay If true, updates file in target that bears the same date as today.
        @param detectMissing If true, missing files not accounted for in the catalog since the earliest date will be included.
        @return A list of _GetToUpdateRet objects.
        """
        
        # Get the target catalog:
        # TODO: Move this direct access to another module to abstract it.
        catalogConn = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        command = {"select": "identifier,collection_date,pointer",
                   "repository": "eq.%s" % self.tgtRepo,
                   "data_source": "eq.%s" % self.datatype,
                   "limit": 100000}
        earliest = self.dateEarliest
        if not detectMissing:
            earliest = lastRunDate
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()

        catResults = catalogConn.select(params=command)
        catResultsSet = set()
        for catResult in catResults:
            ourDate = date_util.localize(arrow.get(catResult["collection_date"]).datetime)
            ourDate = ourDate.replace(hour=0, minute=0, second=0, microsecond=0)
            catResultsSet.add(ourDate)
        # TODO: It would be possible to build up finer-grained control over the catalog. Perhaps we could compare
        # identifiers rather than dates. It could be an option.
        
        ourDatesSet = set()
        for logReader in self.logReaders.values():
            ourDatesSet |= logReader.avail
        
        ourDates = list(ourDatesSet)
        ourDates.sort()
        
        # Build up the list:
        ret = []
        today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
        for ourDate in ourDates:
            ourDate = date_util.localize(ourDate)
            if self.dateEarliest is None or ourDate >= self.dateEarliest:
                if not sameDay and ourDate >= today:
                    continue
                if ourDate < lastRunDate:
                    if not detectMissing:
                        continue
                    if ourDate in catResultsSet:
                        # We are already in the catalog.
                        # TODO: If we were to add a new device, it wouldn't get picked up if we reran this code, until we
                        # have a better way of building up identifiers.
                        continue
                for device, logReader in self.logReaders.items():
                    if logReader.queryDate(ourDate.replace(tzinfo=None)):
                        ret.append(_GetToUpdateRet(device, logReader, ourDate, ourDate < lastRunDate))
        return ret
