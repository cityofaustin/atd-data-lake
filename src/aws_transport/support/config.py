"""
Configuration parameters for the aws_transport package, particularly for accessing the catalog
"""

from aws_transport.support import config_secret
import boto3

# TODO: We may want to create a template and then make the instance of the template not checked in to the repo.

TIMEZONE = "US/Central"

CATALOG_URL = "http://transportation-data-test.austintexas.io/data_lake_catalog"
CATALOG_KEY = getattr(config_secret, "CATALOG_KEY", default="")

KNACK_API_KEY = getattr(config_secret, "KNACK_API_KEY", default="")
KNACK_APP_ID = getattr(config_secret, "KNACK_APP_ID", default="")

AWS_KEY_ID = getattr(config_secret, "AWS_KEY_ID", default="")
AWS_SECRET_KEY = getattr(config_secret, "AWS_SECRET_KEY", default="")

SOC_APP_TOKEN = getattr(config_secret, "SOC_APP_TOKEN", default="")
SOC_WRITE_AUTH = getattr(config_secret, "SOC_WRITE_AUTH", default=())
"SOC_WRITE_AUTH is a tuple of username and password"

SOC_RESOURCE_BT_IAF = "qnpj-zrb9" # "p5mi-kzhb"
SOC_RESOURCE_BT_ITMF = "x44q-icha" # "d8p4-i3md"
SOC_RESOURCE_BT_TMSR = "v7zg-5jg9" # "8m55-9ai3"
SOC_RESOURCE_GS_AGG = "sh59-i6y9"

# KNACK_LOOKUPS provides direct Knack matches to given cross streets found on GRIDSMART devices:
KNACK_LOOKUPS = {"Loop 360_Barton Creek": "LOC16-004315",
                 "Loop 360_Loop 1 SBFR": "LOC16-004350",
                 "Parmer_Market Dwy": "LOC16-002565"}

# STREET_SYNONYMS performs string search/replace to try to match GRIDSMART streets to Knack entries:
STREET_SYNONYMS = {"Loop 1": "MOPAC",
                   "Loop 360": "CAPITAL OF TEXAS HWY",
                   "Hancock Mall": "N IH 35 SVRD SB AT 41ST TRN",
                   "Loop 1": "MOPAC EXPY SVRD"}

def getAWSSession():
    "Uses credentials to connect to AWS. You can then call the resource() method on the return."

    return boto3.Session(aws_access_key_id=AWS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY)
