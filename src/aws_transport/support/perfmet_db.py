"""
perfmet_db.py: Database operations for performance metrics

@author Kenneth Perrine
"""
import datetime

from pypgrest import Postgrest

from util import date_util
from aws_transport.support import config

class PerfMetDB:
    """
    Represents a connection to the PostgREST instance of the performance metrics tables.
    """
    def __init__(self, needsObs=False):
        """
        Initializes the connection to the PostgREST instance.
        """
        self.jobDB = Postgrest(config.PERFMET_JOB_URL, auth=config.CATALOG_KEY)
        self.obsDB = None
        if needsObs:
            self.obsDB = Postgrest(config.PERFMET_OBS_URL, auth=config.CATALOG_KEY)
            
    def writeJob(self, perfMet):
        """
        Writes the job information to the job log.
        """
        metadata = {"data_source": perfMet.dataSource,
                    "stage": perfMet.stage,
                    "seconds": perfMet.processingTotal,
                    "records": perfMet.records,
                    "processing_date": str(perfMet.processingTime),
                    "collection_start": str(date_util.localize(perfMet.collectTimeStart)) if perfMet.collectTimeStart else None,
                    "collection_end": str(date_util.localize(perfMet.collectTimeEnd)) if perfMet.collectTimeEnd else None}
        self.jobDB.upsert(metadata)

    def readAllJobs(self, timestampIn):
        """
        Reads all jobs activity for the given processing day of the timestamp.
        """
        day = date_util.roundDay(date_util.localize(timestampIn))
        command = {"select": "data_source,stage,seconds,records,processing_date,collection_start,collection_end",
                   "processing_date": ["gte.%s" % str(day),
                                       "lt.%s" % str(day + datetime.timedelta(days=1))],
                   "order": "data_source,stage"}
        return self.jobDB.select(params=command)
    
    def getRecentJobsDate(self):
        """
        Returns the most recent processing date for jobs.
        """
        command = {"select": "processing_date",
                   "order": "processing_date.desc",
                   "limit": 1}
        ret = self.jobDB.select(params=command)
        if ret and ret[0] and "processing_date" in ret[0]:
            ret = ret[0]["processing_date"]
        else:
            ret = None
        return ret
    
    def writeObs(self, perfMet):
        """
        Writes observations to the observations log.
        """
        metadata = []
        if not perfMet.observations:
            return
        for identifier, obs in perfMet.observations.items():
            metadata.append({"data_source": perfMet.dataSource,
                             "sensor_name": identifier[0],
                             "data_type": identifier[1],
                             "data": obs.observation,
                             "expected": obs.expected,
                             "collection_date": str(obs.collectionDate),
                             "timestamp_min": str(date_util.localize(obs.minTimestamp)) if obs.minTimestamp else None,
                             "timestamp_max": str(date_util.localize(obs.maxTimestamp)) if obs.maxTimestamp else None})
        self.obsDB.upsert(metadata)
    
    def readAllObs(self, timestampIn, earlyDate=None, dataSource=None, obsType=None):
        """
        Reads all observations activity for the given collection day of the timestamp.
        """
        if not earlyDate:
            timestampIn = date_util.roundDay(date_util.localize(timestampIn))
            earlyDate = timestampIn - datetime.timedelta(days=1)
            collDateClause = ["gte.%s" % str(timestampIn),
                              "lt.%s" % str(timestampIn + datetime.timedelta(days=1))]
        else:
            collDateClause = ["gt.%s" % str(earlyDate),
                              "lte.%s" % str(timestampIn)]
        
        command = {"select": "data_source,sensor_name,data_type,data,expected,collection_date,timestamp_min,timestamp_max",
                   "collection_date": collDateClause,
                   "order": "data_type,sensor_name,collection_date"}
        if dataSource:
            command["data_source"] = "eq.%s" % dataSource
        if obsType:
            command["data_type"] = "eq.%s" % obsType
        return self.obsDB.select(params=command)
        
