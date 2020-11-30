"""
last_update.py: The mechanism for determining what items need to be updated, using source
and target listing objects.

@author Kenneth Perrine
"""
from collections import namedtuple
import datetime

from atd_data_lake.util import date_util

class LastUpdate:
    """
    Performs the comparison between source and target LastUpdate providers.
    """
    # Caveat: While this will allow for targets that have bigger time intervals than sources, the source
    # time intervals must be evenly divisible. It would be possible to allow for partial updates in cases
    # where intervals aren't evenly divisible.
    def __init__(self, source, target=None, force=None):
        """
        Initializes the object
        
        @param source: Used to access information on the availability of source data
        @param target: Used to access information on the availability of target data, or None for all
        @param force: Causes the updater to propose processing on records that are found to already exist
        """
        self.source = source
        self.target = target
        self.force = force
        
        # Set through configure():
        self.startDate = None
        self.endDate = None
        self.baseExtKey = False
        self.baseUnitOpt = True

    def configure(self, startDate=None, endDate=None, baseExtKey=False, baseUnitOpt=True):
        """
        Configures additional properties and parameters for LastUpdate:
        
        @param startDate: Lower bound for finding missing data
        @param endDate: Upper bound for processing, or None for no upper bound
        @param baseExtKey: Set this to true to compare the presence of both base and ext; otherwise, just base is used.
        @param baseUnitOpt: If baseExtKey is False, then if True, prevent tracking of entries that have unit_data.* or site.* extensions.  
        """
        self.startDate = startDate
        self.endDate = endDate
        self.baseExtKey = baseExtKey
        self.baseUnitOpt = baseUnitOpt
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
                if not cmpDateEnd:
                    cmpDateEnd = date_util.localize(cmpDate.replace(tzinfo=None) + datetime.timedelta(days=1))
                if not endDate:
                    endDate = date_util(self.items[self.curIndex].date.replace(tzinfo=None) + datetime.timedelta(days=1))
                if not (cmpDateEnd <= self.items[self.curIndex].date or cmpDate >= endDate):
                    return True
            return False
        
    Identifier = namedtuple("Identifier", "base ext date")
        
    def compare(self, lastRunDate=None):
        """
        Iterates through the source and target, and generates identifiers for those that need updating
        in _LastUpdateItem objects.
        
        @param lastRunDate: Identifies the last run time; but startDate supersedes it as a lower bound if earliest is specified.
        """
        earliest = self.startDate
        if not earliest:
            earliest = lastRunDate
        self.source.prepare(earliest, self.endDate)
        compareTargets = {}
        if self.target:
            self.target.prepare(earliest, self.endDate)
            for target in self.target.runQuery():
                if not self.baseExtKey and self.baseUnitOpt:
                    if target.ext.lower().startswith("unit_data.") or target.ext.lower().startswith("site."):
                        # TODO: This is a quick fix. Consider more robust fixes for this.
                        continue
                key = (target.base, target.ext) if self.baseExtKey else target.base
                if key not in compareTargets:
                    compareTargets[key] = self._CompareTarget()
                compareTargets[key].items.append(target)
        for sourceItem in self.source.runQuery():
            skipFlag = False
            key = (sourceItem.base, sourceItem.ext) if self.baseExtKey else sourceItem.base
            if key in compareTargets:
                compareTarget = compareTargets[key]
                compareTarget.advanceDate(sourceItem.date)
                if compareTarget.isWithin(sourceItem.date, sourceItem.dateEnd):
                    skipFlag = True
            if self.force and skipFlag:
                print("INFO: Forcing processing of %s for date %s." % (str(key), sourceItem.date))
                skipFlag = False
            if not skipFlag:
                yield self._LastUpdateItem(self.Identifier(sourceItem.base, sourceItem.ext, sourceItem.date),
                                      priorLastUpdate=not lastRunDate or sourceItem.date < lastRunDate,
                                      provItem=sourceItem,
                                      label=sourceItem.label)
    
    class _LastUpdateItem:
        """
        Returned from LastUpdate.compare(). Identifies items that need updating.
        """
        def __init__(self, identifier, priorLastUpdate=False, provItem=None, label=None):
            """
            Initializes contents.
            
            @param identifer: A tuple of (base, ext, date)
            @param priorLastUpdate: Set to True if this had been identified outside of the lastUpdate lower bound
            @param provItem: The source data provider item that contains the ".payload" attribute
            @param label: A descriptive label for this item
            """
            self.identifier = identifier
            self.priorLastUpdate = priorLastUpdate
            self.provItem = provItem
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
    def __init__(self, sameDay=False):
        """
        Base constructor.
        
        @param sameDay: If True, allows a last update that happens "today" to be processed, if there is no end date specified.
        """
        self.startDate = None
        self.endDate = None
        self.sameDayDate = date_util.localize(datetime.datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0) \
                        if not sameDay else None
    
    def prepare(self, startDate, endDate):
        """
        Initializes the query between the start date and the end date. If startDate and endDate are
        the same, then only results for that exact time are queried.
        """
        self.startDate = startDate
        self.endDate = endDate
        
    def _getIdentifier(self, base, ext, date):
        """
        Creates identifier for the comparison purposes from the given file information
        
        @return Tuple of base, ext, date
        """
        return base, ext, date

    def runQuery(self):
        """
        Runs a query against the data source and yields results as a generator of _LastUpdProvItem.
        """
        yield from []

    def resolvePayload(self, lastUpdItem):
        """
        Optionally returns a payload contents associated with the lastUpdItem. This can be where an
        expensive query takes place.
        
        @param lastUpdItem: A _LastUpdateItem that contains a ".provItem.payload" attribute
        """
        return lastUpdItem.provItem.payload
        
    "_LastUpdProvItem represents a result from a LastUpdProvider object."
    _LastUpdProvItem = namedtuple("_LastUpdProvItem", "base ext date dateEnd payload label")
    
    def _isSameDayCancel(self, date):
        """
        Returns True if the date is "today", and sameDay processing is disabled. This indicates that
        the proposed match should be withheld because sameDay is False and we're trying to process a 
        record from today.
        """
        return self.sameDayDate and not self.endDate and date >= self.sameDayDate

class LastUpdCatProv(LastUpdProv):
    """
    Represents a Catalog, as a LastUpdate source or target.
    """
    def __init__(self, catalog, repository, baseFilter=None, extFilter=None, sameDay=False):
        """
        Initializes the object
        
        @param baseFilter An exact match string or string containing "%" for pattern-matching base names
        @param extFilter An exact match string or string containing "%" for pattern-matching ext names
        @param sameDay: If False and no endDate is specified, then filter out results that occur "today"
        """
        super().__init__(sameDay=sameDay)
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
                                         exactEarlyDate=(self.startDate == self.endDate)):
            base, ext, date = self._getIdentifier(result["id_base"], result["id_ext"], result["collection_date"])
            if self._isSameDayCancel(date):
                continue
            yield LastUpdProv._LastUpdProvItem(base=base,
                                               ext=ext,
                                               date=date,
                                               dateEnd=result["collection_end"],
                                               payload=result,
                                               label=result["pointer"])

class LastUpdStorageCatProv(LastUpdCatProv):
    """
    Represents a Catalog tied with a Storage object, as a LastUpdate source or target.
    """
    def __init__(self, storage, baseFilter=None, extFilter=None):
        """
        Initializes the object
        """
        super().__init__(storage.catalog, storage.repository, baseFilter, extFilter)

# TODO: It would also be possible to create a provider that generates dates at specified intervals
# for querying a data source that doesn't easily provide its date coverage.
