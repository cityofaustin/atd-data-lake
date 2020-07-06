'''
log_reader contains LogReader for retrieving logs from a GRIDSMART device.

@author: Kenneth Perrine
'''
from __future__ import print_function
import requests
import datetime
import sys
import os

class LogReader:
    '''
    LogReader retrieves logs from a GRIDSMART device.
    '''

    def __init__(self, device):
        """
        Constructs the object and reads in the log date list.
        """
        self.device = device
        self.avail = set()
        self._getLogDates()
        
    def _getLogDates(self):
        """
        _getLogDates internally retrieves the log date list from the device.
        """
        baseURL = self.device.getURL()
        try:
            webResponse = requests.get(baseURL + "counts.json")
        except:
            print("Problem base URL: %s" % baseURL, file=sys.stderr)
            raise

        countsAvail = webResponse.json()
        for item in countsAvail:
            self.avail.add(datetime.datetime.strptime(item, "%Y-%m-%d"))
        
    def queryDate(self, ourDate):
        """
        queryDate returns True if the counts file for the given date is available for download.
        """
        return ourDate in self.avail
    
    def constructBase(self):
        "Returns base part of filename, which is streets."
        
        return self.device.street1 + "_" + self.device.street2
    
    def constructFilename(self, ourDate):
        """
        Returns filename based on device streets and given date.
        """
        return ourDate.strftime("%Y-%m-%d") + "_" + self.constructBase() + ".zip"

    def getCountsFile(self, ourDate, destDir):
        """
        getCountsFile downloads the counts file for the given date (or returns False if not found) and writes according to DATE_Street1_Street2.zip.
        
        @return created file path if successful or None if given date not available.
        """
        if not self.queryDate(ourDate):
            return None
        
        outFilename = self.constructFilename(ourDate)
        print("Writing file: %s" % outFilename)
        baseURL = self.device.getURL()
        ourURL = baseURL + "counts/bydate/%s" % ourDate.strftime("%Y-%m-%d")
        try:
            fileChunks = requests.get(ourURL, stream=True)
        except:
            print("Problem retrieving counts from %s." % ourURL)
            raise
        filePath = os.path.join(destDir, outFilename)
        try:
            with open(filePath, "wb") as outFile:
                for chunk in fileChunks.iter_content(1024 * 1024):
                    outFile.write(chunk)
        except:
            print("Problem writing to %s." % filePath)
            raise
        return filePath
