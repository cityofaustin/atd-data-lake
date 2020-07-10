"""
last_update.py: The mechanism for determining what items need to be updated, using source
and target listing objects.

@author Kenneth Perrine
"""
import datetime

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
        
        @param lastRunDate: Identifies the last run time; used as a lower bound if startDate is None
        """



class _LastUpdateItem:
    """
    Returned from LastUpdate.compare(). Identifies items that need updating.
    """
    def __init__(self, identifier, priorLastUpdate, payload):
        """
        Initializes contents.
        
        @param identifer: A tuple of (base, ext, date)
        @param priorLastUpdate: Set to True if this had been identified outside of the lastUpdate lower bound
        @param payload: Additional identifer or object-specific material that is supplied by the source accessor
        """
        self.identifier = identifier
        self.priorLastUpdate = priorLastUpdate
        self.payload = payload

class LastUpdCatProv:
    """
    Represents a Catalog, as a LastUpdate source or target.
    """
    def __init__(self, catalog, dataSource):
        """
        Initializes the object
        """
        self.catalog = catalog
        self.dataSource = dataSource
        self.startDate = None
        self.endDate = None
        
    def prepare(self, startDate, endDate):
        """
        Initializes the query between the start date and the end date.
        """
        self.startDate = startDate
        self.endDate = endDate
        
    def runQuery(self):
        """
        Runs a query against the data source and generates results.
        """
        
        


class LastUpdStorageProv(LastUpdCatProv):
    """
    Represents a Storage object, as a LastUpdate source or target.
    """
    def __init__(self, storage):
        """
        Initializes the object
        """
        super().__init__(storage.catalog, storage.dataSource)


# TODO: It would also be possible to create a provider that generates dates at specified intervals
# for querying a data source that doesn't easily provide its date coverage.
