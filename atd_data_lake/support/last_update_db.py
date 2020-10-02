"""
last_update_db.py: Support for LastUpdate from a database. Associated database object must have a "query()" method.

@author Kenneth Perrine
"""
import collections

from atd_data_lake.support import last_update

class LastUpdDB(last_update.LastUpdProv):
    """
    Represents a Catalog, as a LastUpdate source or target.
    """
    def __init__(self, dbObject, baseName, extName="csv", sameDay=False):
        """
        Initializes the object

        @param dbObject: Must contain a "query(startDate, endDate)" method that returns a list or dictionary of dates.
        @param baseName: The "base name" that shows up in the identifier object given back by runQuery()
        @param extName: The "ext name" that shows up in the identifier object given back by runQuery()
        @param sameDay: If False and no endDate is specified, then filter out results that occur "today"
        """
        # TODO: Consider being able to pass in filters
        # TODO: Also consider being able to aggregate by increments smaller than a day.
        super().__init__(sameDay=sameDay)
        self.dbObject = dbObject
        self.baseName = baseName
        self.extName = extName
        
    def runQuery(self):
        """
        Runs a query against the data source and provides results as a generator of _LastUpdProvItem.
        """
        lateDate = self.endDate
        if self.startDate == self.endDate:
            lateDate = None
        results = self.dbObject.query(self.startDate, lateDate)
        results = sorted(list(results))
        for result in results:
            base, ext, date = self._getIdentifier(self.baseName, self.extName, result)
            if self._isSameDayCancel(date):
                continue
            filename = self.baseName + "_" + date.strftime("%Y-%m-%d") + "." + self.extName
            yield last_update.LastUpdProv._LastUpdProvItem(base=base,
                                                           ext=ext,
                                                           date=date,
                                                           dateEnd=None,
                                                           payload=result,
                                                           label=filename)

    def resolvePayload(self, lastUpdItem):
        """
        Gets the records for the corresponding lastUpdItem from the database.
        
        @param lastUpdItem: A _LastUpdateItem that contains a date
        """
        ret = self.dbObject.retrieve(earlyDate=lastUpdItem.identifier.date, lateDate=lastUpdItem.identifier.date)
        if isinstance(ret, collections.Mapping):
            ret = ret[lastUpdItem.identifier.date]
        return ret
