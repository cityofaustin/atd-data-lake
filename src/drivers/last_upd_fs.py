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
    def __init__(self, srcDir, pattList, assumeUTC=False, impliedDuration=None):
        """
        Initializes the object.
        
        @param srcDir: A path to the source directory that contains files.
        @param pattList: A list of one or more file patterns captured as util.date_dirs.DateDirDef objects.
        @param assumeUTC: If this is False, assume that dates stored in filenames are local time.
        @param impliedDuration: A datetime.timedelta object that represents a duration of the file, or 1 day if None.
        """
        super().__init__()
        if not isinstance(pattList, Iterable):
            pattList = [pattList]
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
    
    def runQuery(self):
        """
        Runs a query against the data source and provides results as a generator of _LastUpdProvItem.
        """
        # Identify all files that match the dates within the time range:
        for ourDate in self.dateList:
            for index in range(len(self.pattList)):
                myFile = self.dateDirs[index].resolveFile(ourDate.replace(tzinfo=None), fullPath=True)
                if myFile:
                    ext = self.pattList[index].postfix
                    if ext.startswith("."):
                        ext = ext[1:]
                    yield LastUpdProv._LastUpdProvItem(base=self.pattList[index].prefix,
                                           ext=ext,
                                           date=ourDate,
                                           dateEnd=(ourDate + self.impliedDuration) if self.impliedDuration else None,
                                           payload=self.dateDirs[index],
                                           label=myFile)
