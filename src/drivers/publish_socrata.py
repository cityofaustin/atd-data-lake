"""
publish_socrata.py coordinates the writing of data to Socrata

@author Kenneth Perrine
"""
from socratautil
import arrow

from support import publish

SOC_CHUNK = 10000
"SOC_CHUNK is the number of entries per transaction."

class PublishSocrataConn(publish.PublishConnBase):
    """
    Provides a connection to Socrata
    """
    def __init__(self, socrataHost, authKey, socResource, sourceID):
        """
        Initializes object
        """
        self.socrataHost = socrataHost
        self.authKey = authKey
        self.socResource = socResource
        self.sourceID = sourceID
        
    def write(self, dataRows):
        """
        Writes records to the Socrata data resource.
        
        @param dataRows: A list of JSON blocks that correspond with the Socrata field names
        """
        _ = socratautil.Soda(auth=self.authKey,
                             records=dataRows,
                             resource=self.socResource,
                             location_field=None,
                             source=self.sourceID)

    def getPreferredChunk(self):
        """
        Returns the preferred maximum number of rows to write.
        """
        return SOC_CHUNK
    
    def getIdentifier(self):
        """
        Returns the resource identifier, in this case, the Socrata resource
        """
        return self.socResource
    
    def convertTime(self, inTime):
        """
        Converts the datetime object to the time representation that Socrata uses.
        """
        return inTime.strftime("%Y-%m-%dT%H:%M:%S")
