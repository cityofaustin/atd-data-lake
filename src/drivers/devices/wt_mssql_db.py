"""
wt_mssql_db.py contains access details specifically for Wavetronix information that is stored in
the MS SQL database. 

@author Kenneth Perrine
"""
from config import config_wt
import pymssql
import collections

DB_NAME = "KITSDB"

KITSDBRec = collections.namedtuple("KITSDBRec", "detID curDateTime intName detName volume occupancy speed status uploadSuccess detCountComparison dailyCumulative")

class WT_MSSQL_DB:
    """
    Represents a database connection to Wavetronix data stored in an MS SQL database
    """
    # TODO: Consider being able to aggregate by increments smaller than a day.
    def __init__(self):
        """
        Initializes a connection to the database. It will leverage the connection information that's
        stored in the config.config_wt.py file.
        """
        self.conn = pymssql.connect(config_wt.WT_DB_SERVER, config_wt.WT_DB_USER, config_wt.WT_DB_PASSWORD, DB_NAME)
        
    def _buildDatePart(self, earlyDate=None, lateDate=None, includeWhere=False):
        """
        Internal function for building up the date clause on a SQL query.
        """
        ret = ""
        if earlyDate:
            if earlyDate == lateDate:
                ret = "CURDATETIME = '%s'" % str(earlyDate)
            else:
                ret = "CURDATETIME >= '%s'" % str(earlyDate)
        if lateDate and earlyDate != lateDate:
            if ret:
                ret += " AND "
            ret += "CURDATETIME < '%s'" % str(lateDate)
        if ret and includeWhere:
            ret = " WHERE " + ret
        return ret

    def getLatestTimestamp(self, earlyDate=None, lateDate=None):
        """
        Performs a query to determine the latest timestamp encountered that sits between the date range (default: no bound).
        
        @return the latest timestamp, including date and time
        """
        cursor = self.conn.cursor()
        sql = "SELECT TOP 1 CURDATETIME FROM KITSDB.KITS.SYSDETHISTORYRM"
        sql += self._buildDatePart(earlyDate, lateDate, includeWhere=True)
        sql += " ORDER BY CURDATETIME DESC;"
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
            return row[0]
        return None

    def query(self, earlyDate=None, lateDate=None):
        """
        Does a quick search to check for the presence of records. Returns dictionary of days with number of records as values.
        """
        ret = {}
        cursor = self.conn.cursor()
        sql = "SELECT CAST(CURDATETIME AS date), COUNT(1) FROM KITSDB.KITS.SYSDETHISTORYRM"
        sql += self._buildDatePart(earlyDate, lateDate, includeWhere=True)
        sql += " GROUP BY CAST(CURDATETIME AS date);"
        cursor.execute(sql)
        for row in cursor:
            ret[row[0]] = row[1]
        return ret

    def retrieve(self, earlyDate=None, lateDate=None):
        """
        Returns Wavetronix records between the given dates. Returns dictionary of days with list of records as values.
        """
        ret = {}
        cursor = self.conn.cursor()
        sql = """SELECT DETID, CAST(CURDATETIME AS date), CURDATETIME, INTNAME, DETNAME, VOLUME, OCCUPANCY, SPEED, STATUS, UPLOADSUCCESS,
DETCOUNTCOMPARISON, DAILYCUMULATIVE FROM KITSDB.KITS.SYSDETHISTORYRM"""
        sql += self._buildDatePart(earlyDate, lateDate, includeWhere=True)
        sql += "ORDER BY CURDATETIME, INTNAME, DETNAME;"
        cursor.execute(sql)
        for row in cursor:
            rec = KITSDBRec(detID=row[0],
                            curDateTime=row[2],
                            intName=row[3],
                            detName=row[4],
                            volume=row[5],
                            occupancy=row[6],
                            speed=row[7],
                            status=row[8],
                            uploadSuccess=row[9],
                            detCountComparison=row[10],
                            dailyCumulative=row[11])
            if row[0] not in ret:
                ret[row[0]] = []
            ret[row[0]].append(rec)
        return ret
