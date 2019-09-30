"""
unit_data.py: Logic for retrieving device data such as locations-- e.g. data from Knack
"""
import json

import arrow

from util import date_util

class UnitData:
    """
    Utilities for accessing unit data.
    """
    def __init__(self, catalog, s3, bucket, repo, dataSource):
        """
        Store objects that are required for operations with the Data Lake
        """
        self.catalog = catalog
        self.s3 = s3 # TODO: Abstract out AWS
        self.bucket = bucket
        self.repo = repo 
        self.dataSource = dataSource
        self.lastDate = None

    def _buildPath(self, filename, date):
        """
        Builds up an S3 path given the date.
        
        @param filename The name of the file to be stored or retrieved, which may include the date
            based upon the data source naming scheme
        @param date A datetime object signifying the collection date for the given file 
        """
        # TODO: This may be better placed in a repo access helper class.
        year = str(date.year)
        month = str(date.month)
        day = str(date.day)
    
        s_year = year
        s_month = month if len(month) == 2 else month.zfill(2)
        s_day = day if len(day) == 2 else day.zfill(2)
    
        return "{year}/{month}/{day}/{data_source}/{file}".format(year=s_year,
                                                                month=s_month,
                                                                day=s_day,
                                                                data_source=self.dataSource,
                                                                file=filename)

    def getUnitData(self, date):
        """
        Returns a dictionary containing the contents of the most current unit data file.
        Stores the retrieved date into self.lastDate.
        """
        # First, figure out the most applicable unit data file from the catalog.
        command = {"select": "collection_date,pointer",
                   "repository": "eq.%s" % self.repo,
                   "data_source": "eq.%s" % self.dataSource,
                   "identifier": "like.unit_data*", # TODO: Use exact query when we don't use date.
                   "collection_date": "gte.%s" % arrow.get(date).format(),
                   "order": "collection_date",
                   "limit": 1}
        catResults = self.catalog.select(params=command)
        if not catResults:
            # No record found.
            # TODO: We could look for the most recent unit data file up to the date.
            raise Exception("No applicable base unit file found for Repo: %s; Datasource: %s; Date: %s" %
                            (self.repo, self.dataSource, str(date)))
        self.lastDate = date_util.localize(arrow.get(catResults[0]["collection_date"]).datetime)
        unitDataPointer = catResults[0]["pointer"]
        
        # Second, retrieve the unit data file contents from persistent storage.
        # TODO: Abstract this out to a helper object.
        contentObject = self.s3.Object(self.bucket, unitDataPointer)
        fileContent = contentObject.get()['Body'].read().decode('utf-8')
        
        # Third, parse through the JSON and return the contents.
        return json.loads(fileContent)
