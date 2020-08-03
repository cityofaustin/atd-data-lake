"""
last_upd_fs: Implements the last-update functionality for a filesystem

@author Kenneth Perrine
"""
from collections.abc import Iterable

from support.last_update import LastUpdProv
from util import date_dirs
from util import date_util

class LastUpdFileProv(LastUpdProv):
    """
    Represents a filesystem directory where dates and times can be matched from filenames
    """
    def __init__(self, srcDir, pattList, assumeUTC=False, impliedDuration=None, sameDay=False):
        """
        Initializes the object.
        
        @param srcDir: A path to the source directory that contains files.
        @param pattList: A list of one or more file patterns captured as util.date_dirs.DateDirDef objects.
        @param assumeUTC: If this is False, assume that dates stored in filenames are local time.
        @param impliedDuration: A datetime.timedelta object that represents a duration of the file, or 1 day if None.
        @param sameDay: If False and no endDate is specified, then filter out results that occur "today"
        """
        super().__init__(sameDay=sameDay)
        if not isinstance(pattList, Iterable):
            pattList = [pattList]
        self.pattList = pattList
        self.dateDirs = [date_dirs.createDateDir(patt, srcDir) for patt in self.pattList]
        self.assumeUTC = assumeUTC
        self.impliedDuration = impliedDuration
        self.dateList = None
    
    def prepare(self, startDate, endDate):
        """
        Initializes the query between the start date and the end date. If startDate and endDate are
        the same, then only results for that exact time are queried.
        """
        super().prepare(startDate, endDate)

        # Get the unique dates that are within the time range:
        ourDatesSet = set()
        for index in range(len(self.pattList)):
            ourDates = self.dateDirs[index].getDates()
            for ourDate in ourDates:
                ourDate = date_util.localOverwrite(ourDate) if not self.assumeUTC else date_util.localize(ourDate)
                if (not startDate or ourDate >= startDate) \
                        and (not endDate or ourDate < endDate or startDate == endDate and startDate == ourDate):
                    ourDatesSet.add(ourDate)
        self.dateList = list(ourDatesSet)
        self.dateList.sort()
    
    def _getIdentifier(self, filePath, typeIndex, date):
        """
        Creates identifier for the comparison purposes from the given file information
        
        @return Tuple of base, ext
        """
        base = self.pattList[typeIndex].prefix
        ext = self.pattList[typeIndex].postfix
        if ext.startswith("."):
            ext = ext[1:]
        return base, ext, date
    
    def runQuery(self):
        """
        Runs a query against the data source and provides results as a generator of _LastUpdProvItem.
        """
        # Identify all files that match the dates within the time range:
        for ourDate in self.dateList:
            for index in range(len(self.pattList)):
                myFile = self.dateDirs[index].resolveFile(ourDate.replace(tzinfo=None), fullPath=True)
                if myFile:
                    base, ext, date = self._getIdentifier(myFile, index, ourDate)
                    if self._isSameDayCancel(date):
                        continue
                    yield LastUpdProv._LastUpdProvItem(base=base,
                                           ext=ext,
                                           date=date,
                                           dateEnd=(date + self.impliedDuration) if self.impliedDuration else None,
                                           payload=self.dateDirs[index],
                                           label=myFile)
