"""
config_app.py contains application-specific configurations.

@author Kenneth Perrine
"""
from atd_data_lake.config import config_secret, config_support

from atd_data_lake.drivers import storage_s3, catalog_postgrest, perfmet_postgrest, publish_socrata
from atd_data_lake.drivers.devices import bt_unitdata_knack, wt_unitdata_knack, gs_unitdata_knack

# ** These project-wide items are independent of specific devices: **
"Time zone associated with the location of this data lake"
TIMEZONE = "US/Central"

"Unit file location prefix"
UNIT_LOCATION = "Austin"

"Production mode default; use False for debug"
productionMode = True

PURPOSE_REPO_MAP = {"raw-production": "atd-datalake-raw",
                    "raw-debug": "atd-datalake-raw-test",
                    "standardized-production": "atd-datalake-rawjson",
                    "standardized-debug": "atd-datalake-rawjson-test",
                    "ready-production": "atd-datalake-ready",
                    "ready-debug": "atd-datalake-ready-test",
                    "public-production": "socrata",
                    "public-debug": "socrata-test"}

DATASOURCE_MAP = {"bt": config_support.DataSourceConfig(code="bt", name="Bluetooth"),
                  "wt": config_support.DataSourceConfig(code="wt", name="Wavetronix"),
                  "gs": config_support.DataSourceConfig(code="gs", name="GRIDSMART")}

# ** These items are specific to devices and dependencies: **
CATALOG_URL = "https://atd-data-lake.austinmobility.io/data_lake_cat"
CATALOG_KEY = getattr(config_secret, "CATALOG_KEY", "")

PERFMET_JOB_URL = "https://atd-data-lake.austinmobility.io/etl_perfmet_job"
PERFMET_OBS_URL = "https://atd-data-lake.austinmobility.io/etl_perfmet_obs"

KNACK_API_KEY = getattr(config_secret, "KNACK_API_KEY", "")
KNACK_APP_ID = getattr(config_secret, "KNACK_APP_ID", "")

KNACK_PERFMET_ID = getattr(config_secret, "KNACK_PERFMET_ID", "")

AWS_KEY_ID = getattr(config_secret, "AWS_KEY_ID", "")
AWS_SECRET_KEY = getattr(config_secret, "AWS_SECRET_KEY", "")

SOC_HOST = "data.austintexas.gov"
SOC_IDENTIFIER = "datalake"
SOC_APP_TOKEN = getattr(config_secret, "SOC_APP_TOKEN", "")
SOC_WRITE_AUTH = getattr(config_secret, "SOC_WRITE_AUTH", ())
"SOC_WRITE_AUTH is a tuple of username and password"

SOC_RESOURCE_BT_IAF = "qnpj-zrb9"
SOC_RESOURCE_BT_ITMF = "x44q-icha"
SOC_RESOURCE_BT_TMSR = "v7zg-5jg9"
SOC_RESOURCE_GS_AGG = "sh59-i6y9"
SOC_RESOURCE_WT = "i626-g7ub"

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

def createPublisherConn(dataSource, variant=None):
    """
    Returns a new publisher connector object
    """
    if dataSource == "bt":
        if variant == "traf_match_summary":
            socResource = SOC_RESOURCE_BT_TMSR
        elif variant == "matched":
            socResource = SOC_RESOURCE_BT_ITMF
        elif variant == "unmatched":
            socResource = SOC_RESOURCE_BT_IAF
    elif dataSource == "wt":
        socResource = SOC_RESOURCE_WT
    elif dataSource == "gs":
        socResource = SOC_RESOURCE_GS_AGG
    return publish_socrata.PublishSocrataConn(SOC_HOST, SOC_WRITE_AUTH, socResource, SOC_IDENTIFIER)
