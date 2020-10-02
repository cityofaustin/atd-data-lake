"""
perfmet.py: Performance metrics support module for COA ATD Data Lake

@author Kenneth Perrine
"""
import collections
import datetime

from atd_data_lake.util import date_util

SensorObs = collections.namedtuple("SensorObs", "observation expected collectionDate minTimestamp maxTimestamp")

class PerfMet:
    """
    PerfMet class handles the collection and recording of performance metrics.
    """
    def __init__(self, perfmetConn, dataSource, stage):
        """
        Initializes variables to help with the performance metrics recording.
        """
        self.dbConn = perfmetConn
        self.stage = stage
        self.dataSource = dataSource
        self.processingTime = date_util.getNow()
        self.processingTotal = None
        self.records = None
        self.collectTimeStart = None
        self.collectTimeEnd = None
        self.observations = {} # (sensorName, dataType) -> observation
        
    def logJob(self, records):
        """
        Writes a log entry to the "job" database that identifies the update or end of the entire operation.
        """
        # TODO: There could be opportunity to do status updates that can be read by other processes.
        self.processingTotal = (date_util.getNow() - self.processingTime).total_seconds()
        self.records = records
        self.dbConn.writeJob(self)
        
    def recordCollect(self, timestampIn, representsDay=False):
        """
        Tracks the maximum and minimum timestamps. Note that this performs comparisons without localization.
        """
        timestampEnd = timestampIn + datetime.timedelta(days=1) if representsDay else timestampIn
        if not self.collectTimeStart:
            self.collectTimeStart = timestampIn
        if not self.collectTimeEnd:
            self.collectTimeEnd = timestampEnd
        self.collectTimeStart = min(self.collectTimeStart, timestampIn)
        self.collectTimeEnd = max(self.collectTimeEnd, timestampEnd)
    
    def recordSensorObs(self, sensorName, dataType, observation):
        """
        Records an observation. Pass in a SensorObs object.
        """
        self.observations[(sensorName, dataType)] = observation
        
    def writeSensorObs(self):
        """
        Writes sensor observations to the database. Then clears out the cache.
        """
        self.dbConn.writeObs(self)
        self.observations = {}
        