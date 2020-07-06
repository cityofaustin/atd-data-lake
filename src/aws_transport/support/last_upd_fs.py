"""
last_upd_fs.py: Assists with identifying list of last-updated files from the local filesystem given catalog
contents and last-update parameter.

Kenneth Perrine - 2019-02-07
"""

import numbers

from pypgrest import Postgrest
import arrow

from aws_transport.support import config
from util import date_dirs, date_util

class _GetToUpdateRet:
    """
    Return object for LastUpdateFS.getToUpdate()
    """
    def __init__(self, identifier, filePath, fileDate, typeIndex, missingFlag):
        """
        @param identifier A tuple containing identifier_base, identifier_ext, and date.
        @param filePath The full path to the file included in the list.
        @param fileDate A datetime object that signifies the date of the file.
        @param typeIndex The index into the pattList array that's passed into LastUpdateFS that corresponds with this file.
        @param missingFlag Signifies if this file had been detected as missing, preceding the lastRunDate.
        """
        self.identifier = identifier
        self.filePath = filePath
        self.fileDate = fileDate
        self.typeIndex = typeIndex
        self.missingFlag = missingFlag
        
    def __str__(self):
        "Returns string representation."
        return "filePath: '%s'; fileDate: %s; typeIndex: %d; missingFlag: %s" % (self.filePath, str(self.fileDate), \
                                                                                 self.typeIndex, str(self.missingFlag))

class LastUpdateFS:
    """
    Contains methods for creating the list of files to deal with.
    """
    def __init__(self, srcDir, tgtRepo, datatype, pattList, dateEarliest=None):
        """
        @param srcDir The directory to obtain source files.
        @param tgtRepo The code for the targeted repository, for getting date information from the catalog.
        @param dataType The datatype as recorded in the catalog.
        @param pattList A list of one or more file patterns captured as util.date_dirs.DateDirDef objects.
        @param dateEarliest The earliest date to treat as the minimum date to process. None for no earliest, datetime obj., or number for years.
        """
        self.srcDir = srcDir
        self.tgtRepo = tgtRepo
        self.datatype = datatype
        self.pattList = pattList
        
        # Find the earliest date. If it's a number, then it's number of years.
        if isinstance(dateEarliest, numbers.Number):
            dateEarliest = date_util.localize(arrow.now()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .shift(months=-dateEarliest).datetime)
        self.dateEarliest = dateEarliest

    def _getIdentifier(self, filePath, typeIndex, date):
        """
        Returns a tuple containing identifier_base, identifier_ext, and date. The default behavior is to use
        the prefix as defined in the pattern list for the identifier_base, and then use the postfix as defined in
        the pattern list minus the initial dot for the identifier_ext.
        """
        postfix = self.pattList[typeIndex].postfix
        if postfix.startswith("."):
            postfix = postfix[1:]
        return (self.pattList[typeIndex].prefix, postfix, date)
    
    def getToUpdate(self, lastRunDate, sameDay=False, detectMissing=True):
        """
        Yields _GetToUpdateRet for each file that is needing to be selected and updated, given the lastRunDate,
        catalog, and earliest processing date.
        
        @param lastRunDate datetime object that signifies the last run time.
        @param sameDay If true, updates file in target that bears the same date as today.
        @param detectMissing If true, missing files not accounted for in the catalog since the earliest date will be included.
        @yields _GetToUpdateRet object.
        """
        dateDirArr = [date_dirs.createDateDir(d, self.srcDir) for d in self.pattList]
        ourDatesSet = set()
        for index in range(len(self.pattList)):
            ourDates = dateDirArr[index].getDates()
            for ourDate in ourDates:
                ourDatesSet.add(date_util.localOverwrite(ourDate))
        ourDates = list(ourDatesSet)
        ourDates.sort()
        
        # Get the catalog:
        # TODO: Move this direct access to another module to abstract it.
        catalogConn = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        command = {"select": "id_base,id_ext,collection_date,pointer",
                   "repository": "eq.%s" % self.tgtRepo,
                   "data_source": "eq.%s" % self.datatype,
                   "limit": 100000}
        
        earliest = self.dateEarliest
        if not detectMissing:
            earliest = lastRunDate
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()
        # TODO: This is where we'd put in the latest limit if specified, preferably using a helper function.
        catResults = catalogConn.select(params=command)
        catResultsSet = set()
        for catResult in catResults:
            ourDate = date_util.localize(arrow.get(catResult["collection_date"]).datetime)
            ourDate = ourDate.replace(hour=0, minute=0, second=0, microsecond=0)
            catResultsSet.add((catResult["id_base"], catResult["id_ext"], ourDate))
        
        today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
        for ourDate in ourDates:
            if self.dateEarliest is None or ourDate >= self.dateEarliest:
                if not sameDay and ourDate >= today:
                    continue
                if ourDate < lastRunDate:
                    if not detectMissing:
                        continue
                for index in range(len(self.pattList)):
                    identifier = self._getIdentifier(None, index, ourDate)
                    if identifier in catResultsSet:
                        # We are already in the catalog.
                        continue
                    
                    # TODO: Is there a better way to deal with this than to downgrade to naive time? Could we get date_dirs to support time zones?
                    myFile = dateDirArr[index].resolveFile(ourDate.replace(tzinfo=None), fullPath=True)
                    if myFile:
                        yield _GetToUpdateRet(identifier, myFile, ourDate, index, ourDate < lastRunDate)
    