"""
wt_mssql_db.py contains access details specifically for Wavetronix information that is stored in
the MS SQL database. 

@author Kenneth Perrine
"""
from config import config_wt
from util import date_util
import pymssql
import collections
import datetime

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
        self.conn = pymssql.connect(config_wt.WT_DB_SERVER, config_wt.WT_DB_USER, config_wt.WT_DB_PASSWORD, config_wt.WT_DB_NAME)
        
    def _buildDatePart(self, earlyDate=None, lateDate=None, includeWhere=False):
        """
        Internal function for building up the date clause on a SQL query. Strips off time zone information.
        """
        ret = ""
        if earlyDate:
            if earlyDate == lateDate:
                ret = "CAST(CURDATETIME AS date) = CONVERT(VARCHAR, '%s', 120)" % str(earlyDate.strftime("%Y-%m-%d"))
            else:
                ret = "CURDATETIME >= CONVERT(VARCHAR, '%s', 120)" % str(earlyDate.strftime("%Y-%m-%d %H:%M:%S"))
        if lateDate and earlyDate != lateDate:
            if ret:
                ret += " AND "
            ret += "CURDATETIME < CONVERT(VARCHAR, '%s', 120)" % str(lateDate.strftime("%Y-%m-%d %H:%M:%S"))
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
            return date_util.localize(datetime.datetime.strptime(row[0], "%Y-%m-%d"))
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
            ret[date_util.localize(datetime.datetime.strptime(row[0], "%Y-%m-%d"))] = int(row[1])
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
        sql += " ORDER BY CURDATETIME, INTNAME, DETNAME;"
        cursor.execute(sql)
        for row in cursor:
            rec = KITSDBRec(detID=int(row[0]),
                            curDateTime=date_util.localize(row[2]),
                            intName=row[3],
                            detName=row[4],
                            volume=int(row[5]),
                            occupancy=int(row[6]),
                            speed=int(row[7]),
                            status=row[8],
                            uploadSuccess=int(row[9]),
                            detCountComparison=int(row[10]),
                            dailyCumulative=int(row[11]))
            ourDate = date_util.localize(datetime.datetime.strptime(row[1], "%Y-%m-%d"))
            if ourDate not in ret:
                ret[ourDate] = []
            ret[ourDate].append(rec)
        return ret
