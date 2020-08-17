"""
wt_mssql_db.py contains access details specifically for Wavetronix information that is stored in
the MS SQL database. 

@author Kenneth Perrine
"""
from config import config_wt
import pymssql

TABLE = "wt_data"

class WT_MSSQL_DB:
    """
    Represents a database connection to Wavetronix data stored in an MS SQL database
    """
    def __init__(self):
        """
        Initializes a connection to the database. It will leverage the connection information that's
        stored in the config.config_wt.py file.
        """
        self.conn = pymssql.connect(config_wt.WT_DB_SERVER, config_wt.WT_DB_USER, config_wt.WT_DB_PASSWORD, TABLE)
        
    def _buildDatePart(self, earlyDate=None, lateDate=None):
        """
        Internal function for building up the date clause on a SQL query.
        """

    def getLatestTimestamp(self, earlyDate=None, lateDate=None):
        """
        Performs a query to determine the latest timestamp encountered that sits between the date range (default: no bound).
        
        @return the latest timestamp, including date and time
        """
        

    def query(self, earlyDate=None, lateDate=None):
        """
        Does a quick search to check for the presence of records. Returns list sorted by day, and number of records.
        """


    def retrieve(self, earlyDate=None, lateDate=None):
        """
        Returns Wavetronix records between the given dates.
        """
        