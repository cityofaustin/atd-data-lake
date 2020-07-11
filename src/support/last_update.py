"""
last_update.py: The mechanism for determining what items need to be updated, using source
and target listing objects.

@author Kenneth Perrine
"""
from collections import namedtuple

class LastUpdate:
    """
    Performs the comparison between source and target LastUpdate providers.
    """
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
        
        
        
        
        priorLastUpdate = False
        if not self.lastRunDate or ourDate < self.lastRunDate:
            priorLastUpdate = True
        
        
        
        catalogConn = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
        command = {"select": "id_base,id_ext,collection_date,pointer",
                   "repository": "eq.%s" % self.tgtRepo,
                   "data_source": "eq.%s" % self.datatype,
                   "limit": 100000}
        earliest = self.dateEarliest
        if not detectMissing:
            earliest = lastRunDate
        if earliest:
            command["collection_date"] = "gte.%s" % arrow.get(earliest).format()

        catResults = catalogConn.select(params=command)
        catResultsSet = set()
        for catResult in catResults:
            ourDate = date_util.localize(arrow.get(catResult["collection_date"]).datetime)
            ourDate = ourDate.replace(hour=0, minute=0, second=0, microsecond=0)
            catResultsSet.add((catResult["id_base"], catResult["id_ext"], ourDate))
        
        ourDatesSet = set()
        for logReader in self.logReaders.values():
            ourDatesSet |= logReader.avail
        
        ourDates = list(ourDatesSet)
        ourDates.sort()
        # We're driven according to equipment availability.
        
        # Iterate through records
        today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
        for ourDate in ourDates:
            ourDate = date_util.localize(ourDate)
            if self.dateEarliest is None or ourDate >= self.dateEarliest:
                if not sameDay and ourDate >= today:
                    continue
                for device, logReader in self.logReaders.items():
                    identifier = self._getIdentifier(logReader, ourDate)
                    if ourDate < lastRunDate:
                        if not detectMissing:
                            continue
                        if identifier in catResultsSet:
                            # We are already in the catalog.
                            continue
                    if logReader.queryDate(ourDate.replace(tzinfo=None)):
                        yield _GetToUpdateRet(identifier, device, logReader, ourDate, ourDate < lastRunDate)





class _LastUpdateItem:
    """
    Returned from LastUpdate.compare(). Identifies items that need updating.
    """
    def __init__(self, identifier, priorLastUpdate=False, srcPayload=None, tgtPayload=None, label=None):
        """
        Initializes contents.
        
        @param identifer: A tuple of (base, ext, date)
        @param priorLastUpdate: Set to True if this had been identified outside of the lastUpdate lower bound
        @param srcPayload: Additional identifier or object-specific material that is supplied by the source accessor
        @param tgtPayload: Additional identifier or object-specific material that is supplied by the target accessor
        @param label: A descriptive label for this item
        """
        self.identifier = identifier
        self.priorLastUpdate = priorLastUpdate
        self.srcPayload = srcPayload
        self.tgtPayload = tgtPayload
        self.label = label
        
    def __str__(self):
        """
        Offers a descriptive identifier for this match.
        """
        if self.label:
            return self.label
        return "Base: %s; Ext: %s; Date: %s" % (str(self.identifier[0]), str(self.identifier[1]), str(self.identifier[2]))

class LastUpdCatProv:
    """
    Represents a Catalog, as a LastUpdate source or target.
    """
    def __init__(self, catalog, repository, baseFilter=None, extFilter=None):
        """
        Initializes the object
        
        @param baseFilter An exact match string or string containing "%" for matching base names
        @param extFilter An exact match string or string containing "%" for matching ext names
        """
        self.catalog = catalog
        self.repository = repository
        self.baseFilter = baseFilter
        self.extFilter = extFilter
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
        lateDate = self.endDate
        if self.startDate == self.endDate:
            lateDate = None
        for result in self.catalog.query(self.repository, self.baseFilter, self.extFilter, self.startDate, lateDate,
                                         exactEarlyDate=self.startDate == self.endDate):
            yield _LastUpdProvItem(result["id_base"], result["id_ext"], result["collection_date"], payload=result,
                                   label=result["path"])

class LastUpdStorageProv(LastUpdCatProv):
    """
    Represents a Storage object, as a LastUpdate source or target.
    """
    def __init__(self, storage, baseFilter=None, extFilter=None):
        """
        Initializes the object
        """
        super().__init__(storage.catalog, storage.repository, baseFilter, extFilter)

"_LastUpdProvItem represents a result from a LastUpdProvider object."
_LastUpdProvItem = namedtuple("_LastUpdProvItem", "base ext date payload label")

# TODO: It would also be possible to create a provider that generates dates at specified intervals
# for querying a data source that doesn't easily provide its date coverage.
