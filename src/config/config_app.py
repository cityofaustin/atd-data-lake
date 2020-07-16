"""
config_app.py contains application-specific configurations.

@author Kenneth Perrine
"""
import config
from config import config_secret

from drivers import storage_s3, catalog_postgrest, perfmet_postgrest
from drivers.devices import bt_unitdata_knack, wt_unitdata_knack, gs_unitdata_knack

# ** These project-wide items are independent of specific devices: **
TIMEZONE = "US/Central"

"Unit file location prefix"
UNIT_LOCATION = "Austin"

PURPOSE_REPO_MAP = {"raw-production": "atd-data-lake-raw",
                    "raw-debug": "atd-data-lake-raw-test",
                    "canonicalized-production": "atd-data-lake-rawjson",
                    "canonicalized-debug": "atd-data-lake-rawjson-test",
                    "ready-production": "atd-data-lake-ready",
                    "ready-debug": "atd-data-lake-ready-test",
                    "public-production": "socrata",
                    "public-debug": "socrata-test"}

DATASOURCE_MAP = {"bt": config.DataSourceConfig(code="bt", name="Bluetooth"),
                  "wt": config.DataSourceConfig(code="wt", name="Wavetronix"),
                  "gs": config.DataSourceConfig(code="gs", name="GRIDSMART")}

# ** These items are specific to devices and dependencies: **
CATALOG_URL = "http://transportation-data-test.austintexas.io/data_lake_cat_test"
CATALOG_KEY = getattr(config_secret, "CATALOG_KEY", default="")

PERFMET_JOB_URL = "http://transportation-data-test.austintexas.io/etl_perfmet_job"
PERFMET_OBS_URL = "http://transportation-data-test.austintexas.io/etl_perfmet_obs"

KNACK_API_KEY = getattr(config_secret, "KNACK_API_KEY", default="")
KNACK_APP_ID = getattr(config_secret, "KNACK_APP_ID", default="")

KNACK_PERFMET_ID = getattr(config_secret, "KNACK_PERFMET_ID", default="")

AWS_KEY_ID = getattr(config_secret, "AWS_KEY_ID", default="")
AWS_SECRET_KEY = getattr(config_secret, "AWS_SECRET_KEY", default="")

SOC_APP_TOKEN = getattr(config_secret, "SOC_APP_TOKEN", default="")
SOC_WRITE_AUTH = getattr(config_secret, "SOC_WRITE_AUTH", default=())
"SOC_WRITE_AUTH is a tuple of username and password"

SOC_RESOURCE_BT_IAF = "qnpj-zrb9"
SOC_RESOURCE_BT_ITMF = "x44q-icha"
SOC_RESOURCE_BT_TMSR = "v7zg-5jg9"
SOC_RESOURCE_GS_AGG = "sh59-i6y9"

# KNACK_LOOKUPS provides direct Knack matches to given cross streets found on GRIDSMART devices:
KNACK_LOOKUPS = {"Loop 360_Barton Creek": "LOC16-004315",
                 "Loop 360_Loop 1 SBFR": "LOC16-004350",
                 "Parmer_Market Dwy": "LOC16-002565"}

# STREET_SYNONYMS performs string search/replace to try to match GRIDSMART streets to Knack location entries:
STREET_SYNONYMS = {"Loop 1": "MOPAC",
                   "Loop 360": "CAPITAL OF TEXAS HWY",
                   "Hancock Mall": "N IH 35 SVRD SB AT 41ST TRN",
                   "Loop 1": "MOPAC EXPY SVRD"}

# ** These are application- and device-specific factory functions for making connectors **
def createStorageConn(repository):
    """
    Returns a new storage connector object
    """
    if not storage_s3.isAWS_S3_Configured():
        storage_s3.configAWS_S3(AWS_KEY_ID, AWS_SECRET_KEY)
    return storage_s3.StorageS3(repository)
    
def createCatalogConn():
    """
    Returns a new catalog connector object
    """
    return catalog_postgrest.CatalogPostgREST(CATALOG_URL, CATALOG_KEY)
    
def createPerfmetConn():
    """
    Returns a new perfmet connector object
    """
    return perfmet_postgrest.PerfMetDB(PERFMET_JOB_URL, PERFMET_OBS_URL, CATALOG_KEY, needsObs=True)

def createUnitDataConn(dataSource, areaBase):
    """
    Returns a new unit data connector object based on the given dataSource code
    """
    if dataSource == "bt":
        return bt_unitdata_knack.BTUnitDataKnack(KNACK_APP_ID, KNACK_API_KEY, areaBase)
    elif dataSource == "wt":
        return wt_unitdata_knack.WTUnitDataKnack(KNACK_APP_ID, KNACK_API_KEY, areaBase)
    elif dataSource == "gs":
        return gs_unitdata_knack.GSUnitDataKnack(KNACK_APP_ID, KNACK_API_KEY, areaBase)
