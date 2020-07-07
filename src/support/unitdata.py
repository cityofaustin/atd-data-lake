"""
unitData.py: Utility functions and classes for unit data storage and accessing

@author Kenneth Perrine
"""
import datetime
import json

import arrow

from util import date_util

class UnitDataStorage:
    """
    This handles the storage and retrieval of unit data from a Storage object.
    """
    def __init__(self, storageObject, referenceDate=None):
        """
        Initializes the object.
        
        @param storageObject: An initialized Storage object that points to the resource and catalog to retrieve
          from or store to.
        """
        self.storageObject = storageObject
        self.referenceDate = referenceDate
        if 
    
    def retrieve(self):
        """
        This retrieves a unit data dictionary for this data type.
        
        @return Path to the written unit data file if writeFile is true; otherwise, the in-memory dictionary.
        """
        
        buffer = self.storageObject.retrieveBuffer(path)
        return json.loads(buffer)
        # TODO: Re-make the header, or check the integrity of the existing header.

    def store(self, unitData):
        """
        This stores a unit data JSON files for this data type.
        
        @param unitData: Dictionary object of unit data contents 
        """
        
        
def makeHeader(areaBase, device, sameDay=False):
    """
    Utility function for unit data retriever classes that builds up a header
    """
    currentTime = date_util.localize(arrow.now().datetime)
    ourDay = currentTime.replace(hour=0, minute=0, second=0, microsecond=0)
    if not sameDay:
        ourDay =- datetime.timedelta(days=1) 

    targetFilename = "{}_{}_unit_data_{}".format(areaBase, device, ourDay.strftime("%Y-%m-%d"))
    header = {"data_type": "{}_unit_data".format(device),
              "target_filename": targetFilename + ".json",
              "collection_date": str(ourDay),
              "processing_date": str(currentTime)}
    return header

def getIPs(deviceLocations):
    """
    Returns a list of IP addresses from the device locations object.
    """
    try:
        return deviceLocations.device_ip.tolist()
    except Exception as e:
        print(e)
        return None

def createDict(deviceLocations):
    """
    Converts a device locations object to dictionary object.
    """
    try:
        return deviceLocations.apply(lambda row: row.to_dict(), axis=1).tolist()
    except Exception as e:
        print(e)
        return None

        
app.getUnitDataAccessor(purpose)