"""
config package contains application-specific custom configurations. This contains
platform-independent accessors for storage and catalog objects.

@author Kenneth Perrine
"""
from collections import namedtuple

from config import config_app
from support import storage, catalog, unitdata, perfmet, publish

DataSourceConfig = namedtuple("DataSourceConfig", "code name")

def getUnitLcation():
    """
    Returns the unit location as defined in the configuration
    """
    return config_app.UNIT_LOCATION

def getLocalTimezone():
    """
    Returns the local timezone
    """
    return config_app.TIMEZONE

def getRepository(purpose, productionMode=True):
    """
    Returns the repository name given the purpose and production mode status
    """
    lookupStr = purpose + ("-production" if productionMode else "-debug")
    return config_app.PURPOSE_REPO_MAP[lookupStr]

def getDataSourceInfo(dataSourceCode):
    """
    Returns a DataSourceConfig lookup for the given datasource code
    """
    return config_app.DATASOURCE_MAP[dataSourceCode]

def createStorage(catalog, purpose, dataSource, productionMode=True, tempDir=None, simulationMode=False, writeFilePath=None):
    """
    Returns a new storage object implemented according to defs in config_app.py
    """
    repository = getRepository(purpose, productionMode)
    storageConn = config_app.createStorageConn(repository)
    return storage.Storage(storageConn, repository, dataSource, catalog, tempDir, simulationMode, writeFilePath)
    
def createCatalog(dataSource):
    """
    Returns a new catalog object implemented according to defs in config_app.py
    """
    catalogConn = config_app.createCatalogConn()
    return catalog.Catalog(catalogConn, dataSource)
    
def createPerfmet(stageName, dataSource):
    """
    Returns a new perfmet object 
    """
    perfmetConn = config_app.createPerfmetConn()
    return perfmet.PerfMet(perfmetConn, dataSource, stageName)
    
def createUnitDataAccessor(dataSource, areaBase):
    """
    Returns a new unit data object
    """
    return config_app.createUnitDataConn(dataSource, areaBase)

def createPublisher(dataSource, variant, catalog, simulationMode=False, writeFilePath=None):
    """
    Returns a new publisher object.
    
    @param simulationMode: If True, the cloud resource will not be written to
    @param writeFilePath: If a filename is specified here, then publishing will go to the given file rather than a cloud resource
    """
    publisherConn = config_app.createPublisherConn(dataSource, variant)
    return publish.Publisher(publisherConn, catalog, dataSource, simulationMode, writeFilePath)
