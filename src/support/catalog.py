"""
catalog.py: Interface for accessing the Data Lake Catalog

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
import bisect
import datetime

import arrow

from util import date_util

class Catalog:
    """
    Accessors for the Data Lake Catalog.
    """
    def __init__(self, catalogConn, dataSource):
        """
        Initializes catalog connection using the application object
        
        @param catalogConn: An object that establishes the connection to the catalog, a "driver"
        @param dataSource: The code that represents the data source that is being referred
        """
        self.dbConn = catalogConn
        self.dataSource = dataSource
        self.upsertCache = []
        
    def getQueryList(self, stage, base, ext, earlyDate, lateDate, exactEarlyDate=False, limit=None, reverse=False):
        """
        Returns a list of catalog entries sorted by date that match the given criteria.
        
        @param earlyDate: Set this to None to have no early date.
        @param lateDate: Set this to None to have no late date.
        @param exactEarlyDate: Set this to true to query only on exact date defined by the earlyDate parameter
        @param limit: Optional limit on query results
        """
        return [x for x in self.query(stage, base, ext, earlyDate, lateDate, exactEarlyDate=exactEarlyDate, limit=limit, reverse=reverse)]
    
    class _SearchableQueryList:
        """
        The return from getSearchableQueryList() which has a function for returning the index to the next date
        """
        def __init__(self, dates, catalogElements):
            """
            Initializes variables
            """
            self.dates = dates
            self.catalogElements = catalogElements
            
        def getNextDateIndex(self, date):
            """
            Returns the index into catalogElements that has the date equal or immediately next in line from
            the given date.
            """
            return bisect.bisect_left(self.dates, date)
    
    def getSearchableQueryList(self, stage, base, ext, earlyDate, lateDate, exactEarlyDate=False, singleLatest=False):
        """
        Returns a list of catalog entries sorted by date that match the criteria in a tuple along with a simple
        list of dates. This can then be passed into searchQueryListNext to identify the index that corresponds with
        the next date.
        """
        limit = None
        reverse = False
        if singleLatest:
            limit = 1
            reverse = True
        queryList = self.getQueryList(stage, base, ext, earlyDate, lateDate, exactEarlyDate, limit=limit, reverse=reverse)
        
        # Try to get the next one, for good measure:
        if lateDate:
            addedQueryItem = self.queryEarliest(stage, base, ext, lateDate + datetime.timedelta(secs=1))
            if addedQueryItem:
                if not queryList:
                    queryList = [addedQueryItem]
                elif addedQueryItem["collection_date"] != queryList[-1]["collection_date"]:
                    queryList.appen(addedQueryItem)
        
        if queryList:
            return self._SearchableQueryList([x["collection_date"] for x in queryList], queryList)
        return None
    
    def query(self, stage, base, ext, earlyDate, lateDate, exactEarlyDate=False, limit=None, reverse=False):
        """
        Returns a generator of catalog entries sorted by date that match the given criteria.
        
        @param earlyDate: Set this to None to have no early date.
        @param lateDate: Set this to None to have no late date.
        @param exactEarlyDate: Set this to true to query only on exact date defined by the earlyDate parameter
        @param limit: Optional limit on query results
        """
        offset = 0
        while limit is None or offset < limit:
            results = self.dbConn.query(self.dataSource, stage, base, ext, earlyDate, lateDate, \
                exactEarlyDate=exactEarlyDate, limit=self.dbConn.PREFERRED_CHUNK_SIZE, start=offset, reverse=reverse)
            if results:
                for item in results:
                    if item["collection_date"]:
                        item["collection_date"] = date_util.localize(arrow.get(item["collection_date"]).datetime)
                    if item["collection_end"]:
                        item["collection_end"] = date_util.localize(arrow.get(item["collection_end"]).datetime)
                    if item["processing_date"]:
                        item["processing_date"] = date_util.localize(arrow.get(item["processing_date"]).datetime)
                    yield item
            if not results or len(results) < self.dbConn.PREFERRED_CHUNK_SIZE:
                break
            offset += len(results)
        
    def querySingle(self, stage, base, ext, collectionDate):
        """
        Attempts to query for a single item given the criteria.
        """
        results = self.getQueryList(stage, base, ext, collectionDate, exactEarlyDate=True, limit=1)
        return results[0] if results else None
        
    def queryLatest(self, stage, base, ext, earlyDate=None, lateDate=None):
        """
        Returns the latest catalog entry that matches the given criteria, or None if nothing returns.

        @param earlyDate: Limit the search to an early date, or None for no limit.
        @param lateDate: Limit the search to a late date, or None for no limit.
        """
        results = self.getQueryList(stage, base, ext, earlyDate, lateDate, limit=1, reverse=True)
        return results[0] if results else None

    def queryEarliest(self, stage, base, ext, earlyDate=None, lateDate=None):
        """
        Returns the earliest catalog entry that matches the given criteria, or None if nothing returns.

        @param earlyDate: Limit the search to an early date, or None for no limit.
        @param lateDate: Limit the search to a late date, or None for no limit.
        """
        results = self.getQueryList(stage, base, ext, earlyDate, lateDate, limit=1, reverse=False)
        return results[0] if results else None        
        
    def buildCatalogElement(self, stage, base, ext, collectionDate, processingDate, path, collectionEnd=None, metadata=None):
        """
        Builds up a catalog object element from parameters.
        """
        element = {"repository": stage,
                   "data_source": self.dataSource,
                   "id_base": base,
                   "id_ext": ext,
                   "pointer": path,
                   "collection_date": str(collectionDate)}
        if collectionEnd:
            element["collection_end"] = str(collectionEnd)
        if processingDate:
            element["processing_date"] = str(processingDate)
        if metadata:
            element["metadata"] = metadata
        return element
    
    def stageUpsert(self, catalogElement):
        """
        Stages an upsert using a catalog element.
        """
        self.upsertCache.append(catalogElement)
    
    def stageUpsertParams(self, stage, base, ext, collectionDate, processingDate, path, collectionEnd=None, metadata=None):
        """
        Queues a the given item for upsert to the catalog.
        """
        self.stageUpsert(self.buildCatalogElement(stage, base, ext, collectionDate, processingDate, path, collectionEnd, metadata))

    def upsert(self, catalogElement):
        """
        Performs an immediate upsert using the given catalogElement object.
        """
        self.dbConn.upsert(catalogElement)
    
    def upsertParams(self, stage, base, ext, collectionDate, processingDate, path, collectionEnd=None, metadata=None):
        """
        Performs an immediate upsert to the catalog.
        """
        self.upsert(self.buildCatalogElement(stage, base, ext, collectionDate, processingDate, path, collectionEnd, metadata))
    
    def commitUpsert(self):
        """
        Flushes all of the queued upsert items to the catalog.
        """
        self.dbConn.upsert(self.upsertCache)
        self.upsertCache.clear()
        