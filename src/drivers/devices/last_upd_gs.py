"""
last_upd_gs.py: Implements last-update capability for GRIDSMART devices by looking at GRIDSMART history

@author Kenneth Perrine
"""
from support.last_update import LastUpdProv

class LastUpdGSProv(LastUpdProv):
    """
    Represents a collection of GRIDSMART devices and their respective histories
    """
    def __init__(self, devicesLogReaders, targetPath):
        """
        Initializes the object.
        
        @param deviceslogReaders: List of _GSDeviceLogreader objects from gs_support
        @param targetPath: Path to write counts file archives to when getPayload() is called
        """
        super().__init__()
        
        self.devicesLogReaders = devicesLogReaders
        self.targetPath = targetPath
        self.dateList = None

    def prepare(self, startDate, endDate):
        """
        Initializes the query between the start date and the end date. If startDate and endDate are
        the same, then only results for that exact time are queried.
        """
        super().prepare(startDate, endDate)

        # Get the unique dates that are within the time range:
        ourDatesSet = set()
        for device in self.devicesLogReaders:
            for ourDate in device.logReader.avail:
                if (not startDate or ourDate >= startDate) \
                        and (not endDate or ourDate < endDate or startDate == endDate and startDate == ourDate):
                    ourDatesSet.add(ourDate) 
        self.dateList = list(ourDatesSet)
        self.dateList.sort()

    def _getIdentifier(self, logReader, date):
        """
        Creates identifier for the comparison purposes from the given file information
        
        @return Tuple of base, ext
        """
        return logReader.constructBase(), "zip", date
        
    def runQuery(self):
        """
        Runs a query against the data source and provides results as a generator of _LastUpdProvItem.
        """
        # Identify all GRIDSMART records that match the dates within the time range:
        for ourDate in self.dateList:
            for device in self.devicesLogReaders:
                if device.logReader.queryDate(ourDate):
                    base, ext, date = self._getIdentifier(device.logReader, ourDate)
                    yield LastUpdProv._LastUpdProvItem(base=base,
                                                       ext=ext,
                                                       date=date,
                                                       dateEnd=None,
                                                       payload=device,
                                                       label=device.logReader.constructFilename(date))
    
    def getPayload(self, lastUpdItem):
        """
        Optionally returns a payload associated with the lastUpdItem. This can be where an expensive query takes place.
        """
        return lastUpdItem.payload.getCountsFile(lastUpdItem.identifier.date, self.targetPath)
    