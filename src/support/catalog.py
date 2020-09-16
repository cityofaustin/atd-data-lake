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
        self.upsertCache = {}
        
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
            
        def __getitem__(self, index):
            """
            Convenience for retrieving items from the catalogElements array
            """
            return self.catalogElements[index]
            
        def __len__(self):
            """
            Allows the len() function to be used on the object.
            """
            return len(self.catalogElements)

        def getNextDateIndex(self, date):
            """
            Returns the index into catalogElements that has the date equal or immediately greater from
            the given date.
            """
            return bisect.bisect_left(self.dates, date)

        def getNextDateIndexEx(self, date):
            """
            Returns the index into catalogElements that has the date immediately greater from the given date.
            """
            return bisect.bisect_right(self.dates, date)

    def getSearchableQueryList(self, stage, base, ext, earlyDate, lateDate, exactEarlyDate=False, singleLatest=False, baseDict=False):
        """
        Returns a _SearchableQueryList object that contains a list of catalog entries sorted by date that match the criteria.
        The getNextDateIndex() method can be called to identify the index that corresponds with the next date.
        
        @param earlyDate: Set this to None to have no early date.
        @param lateDate: Set this to None to have no late date.
        @param exactEarlyDate: Set this to true to query only on exact date defined by the earlyDate parameter
        @return A single _SearchableQueryList object
        """
        limit = None
        reverse = False
        if singleLatest:
            limit = 1
            reverse = True
        queryList = self.getQueryList(stage, base, ext, earlyDate, lateDate, exactEarlyDate, limit=limit, reverse=reverse)
        
        # Try to get the next one, for good measure:
        if lateDate:
            addedQueryItem = self.queryEarliest(stage, base, ext, lateDate + datetime.timedelta(seconds=1))
            if addedQueryItem:
                if not queryList:
                    queryList = [addedQueryItem]
                elif addedQueryItem["collection_date"] != queryList[-1]["collection_date"]:
                    queryList.appen(addedQueryItem)
        if queryList:
            return self._SearchableQueryList([x["collection_date"] for x in queryList], queryList)
        return None

    class _SearchableQueryDict:
        """
        The return from getSearchableQueryDict() which has a function for returning the element for the next date.
        Also keeps track of whether the return/object is the same.
        """
        def __init__(self, catObj, stage, ext):
            """
            Initializes variables
            
            @param catObj: To allow for impromptu querying if requested element is outside of the dict
            @param stage: Also needed for impromptu querying
            """
            self.prevIndices = {}
            self.searchableLists = {}
            self.catObj = catObj
            self.stage = stage
            self.ext = ext
        
        def getForNextDate(self, base, date, exclusive=False, forceValid=False):
            """
            Returns the catalog element for the next date
            
            @param exlusive: If True, returns the next catalog element if the date matches
            @param forceValud: When True, if the resulting date is out of range, returns the nearest in range
            """
            return self._getForDate(base, date, exclusive, nextFlag=True, forceValid=forceValid)
            
        def getForPrevDate(self, base, date, exclusive=False, forceValid=False):
            """
            Returns the catalog element for the next date
            
            @param exlusive: If True, returns the previous catalog element if the date matches
            @param forceValud: When True, if the resulting date is out of range, returns the nearest in range
            """
            ret = self._getForDate(base, date, exclusive, nextFlag=False, forceValid=forceValid)
            if not ret[0] or base in self.searchableLists and self.searchableLists[base].dates[-1] < date:
                # This happens if the item isn't found from the earlier query. The following will perform a special query.
                sqList = self.catObj.getSearchableQueryList(self.stage, base, self.ext, earlyDate=date, lateDate=None, singleLatest=True)
                if sqList:
                    if base not in self.searchableLists:
                        self.searchableLists[base] = sqList
                    elif self.searchableLists[base].dates[-1] < sqList.dates[0]:
                        self.searchableLists[base].dates.extend(sqList.dates)
                        self.searchableLists[base].catalogElements.extend(sqList.catalogElements)
                    ret = self._getForDate(base, date, exclusive, nextFlag=True, forceValid=forceValid)
            return ret
        
        def _getForDate(self, base, date, exclusive=False, nextFlag=True, forceValid=False):
            """
            Returns the catalog element for the previous or next date
            
            @param nextFlag: If True, returns the catalog element for the next date
            @param exlusive: If True, returns the next catalog element if the date matches
            @param forceValud: When True, if the resulting date is out of range, returns the nearest in range
            @return A tuple of the catalog entry and True if the returned item is new from the last query
            """
            if base not in self.searchableLists:
                return None, False
            searchableList = self.searchableLists[base]
            if not nextFlag:
                if not exclusive:
                    index = searchableList.getNextDateIndexEx(date) - 1
                else:
                    index = searchableList.getNextiDateIndex(date) - 1
            else:
                if not exclusive:
                    index = searchableList.getNextDateIndex(date)
                else:
                    index = searchableList.getNextDateIndexEx(date)
            if index >= len(searchableList):
                if not forceValid:
                    return None, False
                else:
                    index = len(searchableList) - 1
            if index < 0:
                if not forceValid:
                    return None, False
                else:
                    index = 0
            newFlag = False
            if base not in self.prevIndices or index != self.prevIndices[base]:
                self.prevIndices[base] = index
                newFlag = True
            return searchableList[index], newFlag

    def getSearchableQueryDict(self, stage, base, ext, earlyDate, lateDate, exactEarlyDate=False):
        """
        Returns a dictionary of catalog entries keyed by base and sorted by date that match the criteria. The searchQueryListNext()
        method can then be found to find the element that corresponds with the next date.
        
        @param earlyDate: Set this to None to have no early date.
        @param lateDate: Set this to None to have no late date.
        @param exactEarlyDate: Set this to true to query only on exact date defined by the earlyDate parameter
        @return A _SearchableQueryDict object that contains dicts of _SearchableQueryList objects keyed off of base 
        """
        queryList = self.getQueryList(stage, base, ext, earlyDate, lateDate, exactEarlyDate)
        
        ret = self._SearchableQueryDict(self, stage, ext)
        if queryList:
            for item in queryList:
                if item["id_base"] not in ret.searchableLists:
                    ret.searchableLists[item["id_base"]] = self._SearchableQueryList([], [])
                queryListObj = ret.searchableLists[item["id_base"]] 
                queryListObj.dates.append(item["collection_date"])
                queryListObj.catalogElements.append(item)
        return ret
    
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
                exactEarlyDate=exactEarlyDate, limit=self.dbConn.getPreferredChunk(), start=offset, reverse=reverse)
            if results:
                for item in results:
                    if item["collection_date"]:
                        item["collection_date"] = date_util.localize(arrow.get(item["collection_date"]).datetime)
                    if item["collection_end"]:
                        item["collection_end"] = date_util.localize(arrow.get(item["collection_end"]).datetime)
                    if item["processing_date"]:
                        item["processing_date"] = date_util.localize(arrow.get(item["processing_date"]).datetime)
                    yield item
            if not results or len(results) < self.dbConn.getPreferredChunk():
                break
            offset += len(results)
        
    def querySingle(self, stage, base, ext, collectionDate):
        """
        Attempts to query for a single item given the criteria.
        """
        results = self.getQueryList(stage, base, ext, collectionDate, lateDate=None, exactEarlyDate=True, limit=1)
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
        key = (catalogElement["repository"], catalogElement["data_source"], catalogElement["id_base"], catalogElement["id_ext"], catalogElement["collection_date"])
        self.upsertCache[key] = catalogElement # Overwrite if duplicate to avoid problems with PostgREST.
    
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
        if self.upsertCache:
            self.dbConn.upsert(list(self.upsertCache.values()))
            self.upsertCache.clear()
        
