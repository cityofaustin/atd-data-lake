"""
last_upd_soc.py: Assists with identifying Socrata data to update.

Kenneth Perrine - 2019-05-07
"""

import datetime
import numbers

from pypgrest import Postgrest
import arrow
import requests

from aws_transport.support import config
from util import date_util

SOCRATA_URLBASE = "https://data.austintexas.gov"
"The base of the URL for accessing Socrata"

class _GetToUpdateRet:
    """
    Yielded object for LastUpdate_Soc.getToUpdate()
    """
    def __init__(self, s3Path, identifier, fileDate, missingFlag):
        """
        @param s3Path The full path to the file (within the srcRepo bucket) as noted in the catalog.
        @param fileDate A datetime object that signifies the date of the file.
        @param missingFlag Signifies if this file had been detected as missing, preceding the lastRunDate.
        """
        self.s3Path = s3Path
        self.identifier = identifier
        self.fileDate = fileDate
        self.missingFlag = missingFlag
        # TODO: Abstract out S3.

    def __str__(self):
        "Returns string representation."
        return "s3Path: '%s'; fileDate: %s; missingFlag: %s" % (self.s3Path, str(self.fileDate), \
                                                                str(self.missingFlag))

class LastUpdateSoc:
    """
    Contains methods for iterating through catalog records and comparing them with Socrata presence
    """
    def __init__(self, srcRepo, socResource, socDateField, socAuth, dataSource, dateEarliest=12, endDate=None):
        """
        @param srcRepo The code for the source repository, from which we'll be getting filenames.
        @param dataSource The data source as recorded in the catalog.
        @param dateEarliest The earliest date to treat as the minimum date to process; datetime obj., or number for months.
        """
        self.srcRepo = srcRepo
        self.socResource = socResource
        self.socDateField = socDateField
        self.socAuth = socAuth
        self.dataSource = dataSource
        
        # Find the earliest date. If it's a number, then it's number of months.
        if isinstance(dateEarliest, numbers.Number):
            dateEarliest = date_util.localize(arrow.now()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .shift(months=-dateEarliest).datetime)
        self.dateEarliest = dateEarliest

        self.endDate = endDate
        
    def iterToUpdate(self, lastRunDate, sameDay=False, detectMissing=True):
        """
        Yields on each data batch that's needing to be selected and updated, given the lastRunDate,
        catalog, and earliest processing date. These are each yielded as a _GetToUpdateRet object.
        
        @param lastRunDate datetime object that signifies the last run time.
        @param sameDay If true, updates file in target that bears the same date as today.
        @param detectMissing If true, missing records not accounted for in the catalog since the earliest date will be included.
        @yield A _GetToUpdateRet object.
        """
        # Get the catalog:
        catalogConn = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        command = {"select": "identifier,collection_date,pointer",
                   "repository": "eq.%s" % self.srcRepo,
                   "data_source": "eq.%s" % self.dataSource,
                   "order": "collection_date",
                   "limit": 100000}
        
        if not detectMissing:
            earliest = lastRunDate
        if self.dateEarliest and self.dateEarliest > earliest:
            earliest = self.dateEarliest
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()
        if self.endDate:
            clause = "lte.%s" % arrow.get(self.endDate).format()
            if "collection_date" in command:
                command["collection_date"] = [command["collection_date"], clause] # TODO: Is there a better way of promoting to a list if needed?
            else:
                command["collection_date"] = clause
            # TODO: Do we want to assume date-only here?
            
        srcCatResults = catalogConn.select(params=command)
        srcCatResultsDict = {}
        for catResult in srcCatResults:
            ourDate = date_util.localize(arrow.get(catResult["collection_date"]).datetime)
            if ourDate not in srcCatResultsDict:
                srcCatResultsDict[ourDate] = []
            srcCatResultsDict[ourDate].append(catResult)
        # TODO: It would be possible to build up finer-grained control over the catalog. Perhaps we could compare
        # identifiers rather than dates. It could be an option.
        
        # Iterate through Socrata records:
        url = SOCRATA_URLBASE + "/resource/" + self.socResource + ".json"
        today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
        for ourDate in sorted(srcCatResultsDict.keys()):
            if self.dateEarliest is None or ourDate >= self.dateEarliest:
                if not sameDay and ourDate >= today:
                    continue
                if ourDate < lastRunDate:
                    if not detectMissing:
                        continue
                    
                    # Now check for presence in Socrata, and if we get at least one record back, we'll assume we had already
                    # sometime or another populated that day:
                    tomorrow = ourDate + datetime.timedelta(days=1)
                    urlWithDate = url + "/$where=" + self.socDateField + ">='" + ourDate.strftime("%Y-%m-%d") + " AND " \
                        + self.socDateField + "<" + tomorrow.strftime("%Y-%m-%d")
                    res = requests.get(urlWithDate, auth=self.socAuth)
                    if res.json():
                        continue
                    
                for item in srcCatResultsDict[ourDate]:
                    yield _GetToUpdateRet(item["pointer"], item["identifier"], ourDate, ourDate < lastRunDate)
