"""
last_update.py: The mechanism for determining what items need to be updated, using source
and target listing objects.

@author Kenneth Perrine
"""
from collections import namedtuple
import datetime

class LastUpdate:
    """
    Performs the comparison between source and target LastUpdate providers.
    """
    # Caveat: While this will allow for targets that have bigger time intervals than sources, the source
    # time intervals must be evenly divisible. It would be possible to allow for partial updates in cases
    # where intervals aren't evenly divisible.
    def __init__(self, source, target):
        """
        Initializes the object
        
        @param source: Used to access information on the availability of source data
        @param target: Used to access information on the availability of target data
        """
        self.source = source
        self.target = target
        
        # Set through configure():
        self.startDate = None
        self.endDate = None

    def configure(self, startDate=None, endDate=None):
        """
        Configures additional properties and parameters for LastUpdate:
        
        @param startDate: Lower bound for finding missing data
        @param endDate: Upper bound for processing, or None for no upper bound
        """
        self.startDate = startDate
        self.endDate = endDate
        return self

    class _CompareTarget:
        """
        Used in compare() to keep track of targets
        """
        def __init__(self):
            self.items = []
            self.curIndex = 0
            
        def advanceDate(self, cmpDate):
            "Moves the index up to the given date."
            while self.curIndex < len(self.items) and self.items[self.curIndex].date < cmpDate:
                self.curIndex += 1
                
        def isWithin(self, cmpDate, cmpDateEnd):
            "Checks to see if the given date range overlaps the current position."
            if self.curIndex < len(self.items):
                endDate = self.items[self.curIndex].dateEnd
                if not endDate:
                    endDate = self.items[self.curIndex].date + datetime.timedelta(day=1) 
                if not (cmpDateEnd < self.items[self.curIndex].date or cmpDate > cmpDateEnd):
                    return True
            return False
        
    def compare(self, lastRunDate=None):
        """
        Iterates through the source and target, and generates identifiers for those that need updating
        in _LastUpdateItem objects.
        
        @param lastRunDate: Identifies the last run time; used as a lower bound if startDate is None
        """
        earliest = self.startDate
        if not earliest or lastRunDate and earliest > lastRunDate:
            earliest = lastRunDate
        self.source.prepare(earliest, self.endDate)
        self.target.prepare(earliest, self.endDate)
        compareTargets = {}
        for target in self.target.runQuery():
            key = (target.base, target.ext)
            if key not in compareTargets:
                compareTargets[key] = self._CompareTarget()
            compareTargets[key].items.append(target)
        for sourceItem in self.source.runQuery():
            skipFlag = False
            key = (sourceItem.base, sourceItem.ext)
            if key in compareTargets:
                compareTarget = compareTargets[key]
                compareTarget.advanceDate(sourceItem.date)
                if compareTarget.isWithin(sourceItem.date, sourceItem.dateEnd):
                    skipFlag = True
            if not skipFlag:
                yield self._LastUpdateItem((sourceItem.base, sourceItem.ext, sourceItem.date),
                                      priorLastUpdate=not self.lastRunDate or sourceItem.date < self.lastRunDate,
                                      payload=self.source.getPayload(sourceItem),
                                      label=sourceItem.label)
    
    class _LastUpdateItem:
        """
        Returned from LastUpdate.compare(). Identifies items that need updating.
        """
        def __init__(self, identifier, priorLastUpdate=False, payload=None, label=None):
            """
            Initializes contents.
            
            @param identifer: A tuple of (base, ext, date)
            @param priorLastUpdate: Set to True if this had been identified outside of the lastUpdate lower bound
            @param payload: Additional identifier or object-specific material that is supplied by the source accessor
            @param label: A descriptive label for this item
            """
            self.identifier = identifier
            self.priorLastUpdate = priorLastUpdate
            self.payload = payload
            self.label = label
            
        def __str__(self):
            """
            Offers a descriptive identifier for this match.
            """
            if self.label:
                return self.label
            return "Base: %s; Ext: %s; Date: %s" % (str(self.identifier[0]), str(self.identifier[1]), str(self.identifier[2]))
    
    # TODO: Would we ever need this for items that do exist in the target?

class LastUpdProv:
    """
    Base class for provision of last-update information
    """
    def __init__(self):
        """
        Base constructor.
        """
        self.startDate = None
        self.endDate = None
    
    def prepare(self, startDate, endDate):
        """
        Initializes the query between the start date and the end date. If startDate and endDate are
        the same, then only results for that exact time are queried.
        """
        self.startDate = startDate
        self.endDate = endDate
        
    def runQuery(self):
        """
        Runs a query against the data source and provides results as a generator of _LastUpdProvItem.
        """
        pass
        
    def getPayload(self, lastUpdItem):
        """
        Optionally returns a payload associated with the lastUpdItem. This can be where an expensive query takes place.
        """
        return lastUpdItem.payload
    
    "_LastUpdProvItem represents a result from a LastUpdProvider object."
    _LastUpdProvItem = namedtuple("_LastUpdProvItem", "base ext date dateEnd payload label")

class LastUpdCatProv(LastUpdProv):
    """
    Represents a Catalog, as a LastUpdate source or target.
    """
    def __init__(self, catalog, repository, baseFilter=None, extFilter=None):
        """
        Initializes the object
        
        @param baseFilter An exact match string or string containing "%" for matching base names
        @param extFilter An exact match string or string containing "%" for matching ext names
        """
        super().__init__()
        self.catalog = catalog
        self.repository = repository
        self.baseFilter = baseFilter
        self.extFilter = extFilter
        
    def runQuery(self):
        """
        Runs a query against the data source and provides results as a generator of _LastUpdProvItem.
        """
        lateDate = self.endDate
        if self.startDate == self.endDate:
            lateDate = None
        for result in self.catalog.query(self.repository, self.baseFilter, self.extFilter, self.startDate, lateDate,
                                         exactEarlyDate=self.startDate == self.endDate):
            yield LastUpdProv._LastUpdProvItem(result["id_base"], result["id_ext"], result["collection_date"], result["collection_end"],
                                   payload=result, label=result["path"])

class LastUpdStorageProv(LastUpdCatProv):
    """
    Represents a Storage object, as a LastUpdate source or target.
    """
    def __init__(self, storage, baseFilter=None, extFilter=None):
        """
        Initializes the object
        """
        super().__init__(storage.catalog, storage.repository, baseFilter, extFilter)

# TODO: It would also be possible to create a provider that generates dates at specified intervals
# for querying a data source that doesn't easily provide its date coverage.