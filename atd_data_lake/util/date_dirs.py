'''
Date directories processes lists of files to retrieve dates as represented in filenames.

@author: Kenneth Perrine
'''
import datetime
import os
import bisect

class DateDirDef:
    """
    Intended for capturing the initializer parameters for DateDir.
    """
    
    def __init__(self, prefix="", dateFormat="%Y_%m_%d", postfix="", findDirs=False):
        """
        @param prefix: The filename pattern that precedes the date
        @param dateFormat: The format of the date that is expected: See https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior
        @param postfix: The filename pattern that goes after the date
        @param findDirs: If True, matches only directory names instead of filenames
        """
        self.prefix = prefix
        self.dateFormat = dateFormat
        self.postfix = postfix
        self.findDirs = findDirs

def createDateDir(dateDirDef, path="."):
    """
    Creates DateDir from the given DateDirDef object.
    """
    return DateDir(path, dateDirDef.prefix, dateDirDef.dateFormat, dateDirDef.postfix, dateDirDef.findDirs)

class DateDir:
    """
    DateDir
    """

    def __init__(self, path=".", prefix="", dateFormat="%Y_%m_%d", postfix="", findDirs=False):
        """
        The initializer will kick off the file listing at the given path, collecting together all of the dates that match the pattern.
        
        @param path: The path to the directory of interest
        @param prefix: The filename pattern that precedes the date
        @param dateFormat: The format of the date that is expected: See https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior
        @param postfix: The filename pattern that goes after the date
        @param findDirs: If True, matches only directory names instead of filenames
        """
        self.path = path
        self.prefix = prefix
        self.dateFormat = dateFormat
        self.postfix = postfix
        self.findDirs = findDirs
        self._readDir()
                
    def _readDir(self):
        """
        _readDir is an internal function that reads the contents of a directory.
        """
        self.dirList = []
        dupSet = set()
        postfixPos = -len(self.postfix) if self.postfix else None 
        for filename in os.listdir(self.path):
            if filename.startswith(self.prefix) and filename.endswith(self.postfix):
                fullFile = os.path.join(self.path, filename) 
                if self.findDirs and os.path.isdir(fullFile) or not self.findDirs and os.path.isfile(fullFile):
                    try:
                        ourDate = datetime.datetime.strptime(filename[len(self.prefix):postfixPos], self.dateFormat)
                        if ourDate not in dupSet:
                            self.dirList.append((ourDate, filename))
                            dupSet.add(ourDate)
                        else:
                            print("Duplicate match found for file " + filename) 
                    except ValueError:
                        pass
        self.dirList.sort()
        
    def getDates(self):
        """
        getDates returns all of the dates in the directory.
        """
        return [item[0] for item in self.dirList]
    
    def getMostRecentDate(self):
        """
        getMostRecentDate returns the most recent date that was found, or None if no files found.
        """
        return self.dirList[-1][0] if self.dirList else None
    
    def resolveFile(self, ourDate, fullPath=False, mostRecent=False, nextFuture=False):
        """
        resolveFile returns the filename that corresponds with the give date, or None if not found.
        
        @param ourDate: The date to query.
        @param fullPath: If True, returns the full path to the filename.
        @param mostRecent: If True, returns the most recent filename up to the given date if no exact match is found.
        @param nextFuture: If True, returns the next future filename if no exact match is found. 
        """
        if mostRecent and nextFuture:
            return None
        if not self.dirList:
            return None
        ret = None
        index = bisect.bisect_left(self.dirList, (ourDate,))
        if index < len(self.dirList) and (self.dirList[index][0] == ourDate or nextFuture):
            ret = self.dirList[index][1]
        if mostRecent and index > 0:
            ret = self.dirList[index - 1][1]
        if ret and fullPath:
            ret = os.path.join(self.path, ret)
        return ret
    
    def queryDate(self, ourDate):
        """
        queryDate returns True if the given date exists in the list.
        """
        return True if self.resolveFile(ourDate) else False
    
    def getAllFiles(self, fullPath=False):
        """
        getAllFiles returns all files in the directory regardless of date.
        
        @param fullPath: If True, returns the full path to the filename.
        """
        return self.getFileRange(fullPath=fullPath)
    
    def getNewerFiles(self, ourDate, inclusive=True, fullPath=False):
        """
        getNewerFiles returns a list of filenames that are newer than the given date
        
        @param ourDate: The date to query.
        @param inclusive: If True, also includes the current date if there is an exact match.
        @param fullPath: If True, returns the full path to the filename.
        """
        return self.getFileRange(earlyDate=ourDate, inclusive=inclusive, fullPath=fullPath)

    def getOlderFiles(self, ourDate, inclusive=True, fullPath=False):
        """
        getOlderFiles returns a list of filenames that are older than the given date
        
        @param ourDate: The date to query.
        @param inclusive: If True, also includes the current date if there is an exact match.
        @param fullPath: If True, returns the full path to the filename.
        """
        return self.getFileRange(lateDate=ourDate, inclusive=inclusive, fullPath=fullPath)
    
    def getFileRange(self, earlyDate=None, lateDate=None, inclusive=True, fullPath=False):
        """
        getFileRange returns a list of filenames between the two given dates.
        
        @param earlyDate: The earlier date to query.
        @param lateDate: The later date to query.
        @param inclusive: If True, also includes the current date if there is an exact match.
        @param fullPath: If True, returns the full path to the filename.
        """
        if (not earlyDate and not lateDate) or not self.dirList:
            return []
        lowIndex = None
        if earlyDate:
            lowIndex = bisect.bisect_left(self.dirList, (earlyDate,))
            if not inclusive and self.dirList[lowIndex][0] == earlyDate:
                lowIndex += 1
        highIndex = None
        if lateDate:
            highIndex = bisect.bisect_left(self.dirList, (lateDate,))
            if highIndex < len(self.dirList) and inclusive and self.dirList[highIndex][0] == lateDate:
                highIndex += 1
        ret = [item[1] for item in self.dirList[lowIndex:highIndex]]
        if fullPath:
            ret = [os.path.join(self.path, item) for item in ret]
        return ret
        
