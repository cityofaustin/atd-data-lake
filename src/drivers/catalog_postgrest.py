"""
catalog_postgrest.py: Catalog functions facilitated by calls to PostgREST

Kenneth Perrine
Center for Transportation Research, The University of Texas at Austin
"""
from pypgrest import Postgrest

"PREFERRED_CHUNK_SIZE is the number of records that are preferred to be returned in a multi-record query."
PREFERRED_CHUNK_SIZE = 10000

class CatalogPostgREST:
    """
    Implements catalog access functions using PostgREST.
    """
    def __init__(self, accessPoint, apiKey):
        """
        Initializes the PostgREST access with a given access point URL and the API key.
        """
        self.catalogDB = Postgrest(accessPoint, auth=apiKey)
        
    def query(self, dataSource, stage, base, ext, earlyDate=None, lateDate=None, exactEarlyDate=False, limit=None, start=None, reverse=False):
        """
        Performs a query on the given datatype, data stage, base, ext, and optional early and late dates. Returns a list
        of dictionary objects, each a result.
        
        @param exactEarlyDate: Set this to true to query only on exact date defined by the earlyDate parameter
        @param limit Limits the output to a specific number of records. If None, then the driver default is used.
        @param start sets the start frome wnen doing a multi-chunk query.
        @param reverse will allow the results to be sorted in descending order.
        """
        # TODO: Do we need a query that will return a catalog entry that contains a given collection date (between collection_date
        # and collection_end)?
        
        # Specify query plus required parameters and sorting/pagination parameters:
        command = {"select": "collection_date,collection_end,processing_date,pointer,id_base,id_ext,metadata",
            "repository": "eq.%s" % stage,
            "data_source": "eq.%s" % dataSource,
            "order": ("collection_date.asc" if not reverse else "collection_date.desc") + ",id_base.asc,id_ext.asc",
            "limit": 1 if limit is None else limit,
            "offset": 0 if start is None else start}
        
        # Allow base and ext identifiers to be omitted, or to be a "match first part of string" query:
        if base is not None:
            if "%%" in base:
                command["id_base"] = "like.%s" % base.replace("%%", "*")
            else:
                command["id_base"] = "eq.%s" % base
        if ext is not None:
            if "%%" in ext:
                command["id_ext"] = "like.%s" % ext.replace("%%", "*")
            else:
                command["id_ext"] = "eq.%s" % ext
                
        # Collection date range: May need to use an array because there could be two constraints:
        collDateRange = []
        if earlyDate is not None:
            if exactEarlyDate:
                collDateRange.append("eq.%s" % str(earlyDate))
            else:    
                collDateRange.append("gte.%s" % str(earlyDate))
        if lateDate is not None:
            collDateRange.append("lt.%s" % str(lateDate))
        if collDateRange:
            if len(collDateRange) == 1:
                command["collection_date"] = collDateRange[0]
            else:
                command["collection_date"] = collDateRange
                
        # Run the query:
        return self.catalogDB.select(params=command)
    
    def upsert(self, upsertDataList):
        """
        Performs an upsert operation on the given list of dictionary objects. Each dictionary object shall contain
        "repository", "data_source", "id_base", "id_ext", "pointer", "collection_date", "collection_end" (optional),
        "processing_date", and optionally "metadata".
        """
        self.catalogDB.upsert(upsertDataList)
    
    @staticmethod
    def getPreferredChunk():
        """
        Retruns the preferred chunk size that catalog.Catalog.query() should used in requests.
        """
        return PREFERRED_CHUNK_SIZE
