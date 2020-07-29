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
    def __init__(self, storageObject, areaBase):
        """
        Initializes the object.
        
        @param storageObject: An initialized Storage object that points to the resource and catalog to retrieve
          from or store to.
        """
        self.storageObject = storageObject
        self.areaBase = areaBase
        self.unitDataCatList = None
        self.prevIndex = None
        self.prevUnitData = None
    
    def prepare(self, dateEarliest=None, dateLatest=None):
        """
        Searches the catalog for relevant unit data that is relevant to the date constraints if given
        """
        self.unitDataCatList = self.storageObject.catalog.getSearchableQueryList(self.storage.repository, self.areaBase,
                                            "unit_data.json", dateEarliest, dateLatest,
                                            exactEarlyDate=(dateEarliest and dateEarliest == dateLatest),
                                            singleLatest=(not dateEarliest and not dateLatest))
        return self
    
    def retrieve(self, date=None):
        """
        This retrieves a unit data dictionary for this data type.
        
        @return Path to the written unit data file if writeFile is true; otherwise, the in-memory dictionary.
        """
        # If prepare was never called, we'll just retrieve the latest unit data:
        if not self.unitDataCatList:
            self.prepare()
        
        # Find unit data catalog entry and return efficient responses if they're cached:
        if date:
            date += datetime.timedelta(secs=1)
        unitDataCatIndex = self.unitDataCatList.getNextDateIndex(date) if date else len(self.unitDataCatList.catalogElements) - 1 
        if unitDataCatIndex >= len(self.unitDataCatList.catalogElements) or unitDataCatIndex < 0:
            return None
        if unitDataCatIndex == self.prevIndex:
            return self.prevUnitData
        
        # Get the unit data:
        buffer = self.storageObject.retrieveBuffer(self.unitDataCatList.catalogElements[unitDataCatIndex]["pointer"])
        self.prevIndex = unitDataCatIndex
        self.prevUnitData = json.loads(buffer)
        return self.prevUnitData
        # TODO: Re-make the header, or check the integrity of the existing header.

    def store(self, unitData):
        """
        This stores a unit data JSON file for this data type.
        
        @param unitData: Dictionary object of unit data contents 
        """
        unitDataCat = self.storageObject.createCatalogElement(self.areaBase, "unit_data.json",
            unitData["header"]["collection_date"], processingDate=unitData["header"]["collection_date"],
            metadata=unitData["header"])
        # TODO: If unit data gets big, we'll need to see if it is better to write to a file and write that out.
        self.storageObject.writeJSON(unitData, unitDataCat)
        
def makeHeader(areaBase, device, sameDay=False):
    """
    Utility function for unit data retriever classes that builds up a header
    """
    currentTime = date_util.localize(arrow.now().datetime)
    ourDay = currentTime.replace(hour=0, minute=0, second=0, microsecond=0)
    if not sameDay:
        ourDay -= datetime.timedelta(days=1) 

    targetFilename = "{}_{}_{}_unit_data".format(areaBase, device, ourDay.strftime("%Y-%m-%d"))
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
