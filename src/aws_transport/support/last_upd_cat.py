"""
last_upd_cat.py: Assists with identifying list of last-updated files from the catalog given catalog contents
and last-update parameter.

Kenneth Perrine - 2019-02-07
"""

from aws_transport.support import config
from util import date_util
from pypgrest import Postgrest
import arrow
import numbers

class _GetToUpdateRet:
    """
    Return object for LastUpdateCat.getToUpdate()
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
        
class LastUpdateCat:
    """
    Contains methods for creating the list of files to deal with.
    """
    def __init__(self, srcRepo, tgtRepo, datatype, dateEarliest=None):
        """
        @param srcRepo The code for the source repository, from which we'll be getting filenames.
        @param tgtRepo The code for the targeted repository, for getting date information from the catalog.
        @param dataType The datatype as recorded in the catalog.
        @param dateEarliest The earliest date to treat as the minimum date to process. None for no earliest, datetime obj., or number for months.
        """
        self.srcRepo = srcRepo
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
        Returns a list of all files that are needing to be selected and updated, given the lastRunDate,
        catalog, and earliest processing date. These are returned as a list of _GetToUpdateRet objects.
        
        @param lastRunDate datetime object that signifies the last run time.
        @param sameDay If true, updates file in target that bears the same date as today.
        @param detectMissing If true, missing files not accounted for in the catalog since the earliest date will be included.
        @return A list of _GetToUpdateRet objects.
        """
        
        # Get the source catalog:
        # TODO: Move this direct access to another module to abstract it.
        catalogConn = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        command = {"select": "identifier,collection_date,pointer",
                   "repository": "eq.%s" % self.srcRepo,
                   "data_source": "eq.%s" % self.datatype,
                   "order": "collection_date", # TODO: Consider backward.
                   "limit": 1000000} # TODO: We need a better way. Smaller chunks; query when needed.
        earliest = self.dateEarliest
        if not detectMissing:
            earliest = lastRunDate
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()

        srcCatResults = catalogConn.select(params=command)
        srcCatResultsDict = {}
        for catResult in srcCatResults:
            ourDate = date_util.localize(arrow.get(catResult["collection_date"]).datetime)
            if ourDate not in srcCatResultsDict:
                srcCatResultsDict[ourDate] = []
            srcCatResultsDict[ourDate].append(catResult)
        # TODO: It would be possible to build up finer-grained control over the catalog. Perhaps we could compare
        # identifiers rather than dates. It could be an option.
        
        srcCatList = list(srcCatResultsDict.keys())
        srcCatList.sort()
        
        # Get the target catalog:
        # TODO: Will the target catalog ever be on a different server than the source catalog?
        # TODO: Again, move this direct access to another module to abstract it.
        command = {"select": "identifier,collection_date,pointer",
                   "repository": "eq.%s" % self.tgtRepo,
                   "data_source": "eq.%s" % self.datatype,
                   "order": ["collection_date", "identifier"],
                   "limit": 1000000} # TODO: We need a better way. Smaller chunks; query when needed.
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()

        tgtCatResults = catalogConn.select(params=command)
        tgtCatResultsSet = set()
        for catResult in tgtCatResults:
            tgtCatResultsSet.add(date_util.localize(arrow.get(catResult["collection_date"]).datetime))
        # TODO: Again, figure out what to key off of.
        
        # Build up the list:
        ret = []
        today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
        for ourDate in srcCatList:
            if self.dateEarliest is None or ourDate >= self.dateEarliest:
                if not sameDay and ourDate >= today:
                    continue
                if ourDate < lastRunDate:
                    if not detectMissing:
                        continue
                    if ourDate in tgtCatResultsSet:
                        # We are already in the catalog.
                        # TODO: If we were to add a new file, it wouldn't get picked up if we reran this code, until we
                        # have a better way of building up identifiers.
                        continue
                for item in srcCatResultsDict[ourDate]:
                    ret.append(_GetToUpdateRet(item["pointer"], item["identifier"], ourDate, ourDate < lastRunDate))
        return ret
