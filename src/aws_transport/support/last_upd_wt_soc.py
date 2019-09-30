"""
last_upd_wt_soc.py: Assists with identifying and fetching Wavetronix Socrata data. Unlike the other last_upd scripts, this experiment
uses yield to present actual data.

TODO: We can generalize this to Socrata data.

Kenneth Perrine - 2019-02-15
"""

import datetime
import numbers

from pypgrest import Postgrest
import arrow

from aws_transport.support import config
from collecting.wt import socrata_history
from util import date_util

class _GetToUpdateRet:
    """
    Yielded object for LastUpdateWT_Soc.getToUpdate()
    """
    def __init__(self, filePath, fileDate, missingFlag):
        """
        @param filePath The full path to the file included in the list. It's already written.
        @param fileDate A datetime object that signifies the date of the file.
        @param missingFlag Signifies if this file had been detected as missing, preceding the lastRunDate.
        """
        self.filePath = filePath
        self.fileDate = fileDate
        self.missingFlag = missingFlag
        
    def __str__(self):
        "Returns string representation."
        return "filePath: '%s'; fileDate: %s; missingFlag: %s" % (self.filePath, str(self.fileDate), \
                                                                  str(self.missingFlag))

class LastUpdateWT_Soc:
    """
    Contains methods for iterating through Wavetronix Socrata records
    """
    def __init__(self, appToken, tgtRepo, dataSource, tgtPath, dateEarliest=12):
        """
        @param srcDir The directory to obtain source files.
        @param tgtRepo The code for the targeted repository, for getting date information from the catalog.
        @param dataSource The data source as recorded in the catalog.
        @param dateEarliest The earliest date to treat as the minimum date to process; datetime obj., or number for months.
        """
        self.appToken = appToken
        self.tgtRepo = tgtRepo
        self.tgtPath = tgtPath
        self.dataSource = dataSource
        
        # Fix up the app token:
        socrata_history.setAppToken(config.SOC_APP_TOKEN)
        
        # Find the earliest date. If it's a number, then it's number of months.
        if isinstance(dateEarliest, numbers.Number):
            dateEarliest = date_util.localize(arrow.now()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .shift(months=-dateEarliest).datetime)
        self.dateEarliest = dateEarliest
        
    def iterToUpdate(self, lastRunDate, force=False, detectMissing=True):
        """
        Yields on each data batch that's needing to be selected and updated, given the lastRunDate,
        catalog, and earliest processing date. These are each yielded as a _GetToUpdateRet object.
        
        @param lastRunDate datetime object that signifies the last run time.
        @param force If true, updates files even if file is present in target.
        @param detectMissing If true, missing records not accounted for in the catalog since the earliest date will be included.
        @yield A _GetToUpdateRet object.
        """
        # Get the catalog:
        catalogConn = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        command = {"select": "identifier,collection_date,pointer",
                   "repository": "eq.%s" % self.tgtRepo,
                   "data_source": "eq.%s" % self.dataSource,
                   "limit": 100000}
        
        if not detectMissing:
            earliest = lastRunDate
        if self.dateEarliest and self.dateEarliest > earliest:
            earliest = self.dateEarliest
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()
        catResults = catalogConn.select(params=command)
        catResultsSet = set()
        for catResult in catResults:
            ourDate = date_util.localize(arrow.get(catResult["collection_date"]).datetime)
            ourDate = ourDate.replace(hour=0, minute=0, second=0, microsecond=0)
            catResultsSet.add(ourDate)
        
        # Iterate through Socrata records:
        delta = arrow.now().datetime - earliest
        for nday in range(delta.days):
            ourDate = earliest + datetime.timedelta(days=nday)
            if ourDate not in catResultsSet or force and ourDate >= self.dateEarliest:
                smallSample = socrata_history.getDataAt(ourDate, limit=1)
                if smallSample:
                    filePath = socrata_history.getDataAt(ourDate, outPath=self.tgtPath)
                    yield _GetToUpdateRet(filePath, ourDate, ourDate < lastRunDate) 
