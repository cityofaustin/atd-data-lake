import os
from json import dumps
import tempfile
import shutil
import datetime

from pypgrest import Postgrest
import arrow

import _setpath
from aws_transport.support import config
from util import date_util
from aws_transport.support.knack_access import get_device_locations

"S3 bucket to target"
TGT_BUCKET = config.composeBucket("rawjson")

"Temporary directory holding-place"
_TEMP_DIR = None

"S3 object"
_S3 = None

class unitLocations_toBucket2:
    ''' 'Class calls knack_access and places location information in Austin's Data Lake bucket 2
        (Change tgtBucket and tgtRepo for other locations) '''

    def __init__(self, unit_type, baseName, tgtDate, tgtStoragePath, areaBase, catalog):

        self.unit_type = unit_type
        self.baseName = baseName #format is unit_locations_YYYY-mm-dd
        self.tgtStoragePath = tgtStoragePath # TODO: Maybe let storagePath just be the S3 path: use baseName.
        # TODO: Provide standardized method to reconstruct the S3 path.
        self.collection_date = str(tgtDate) #str(date_util.localize(arrow.now().datetime))
        self.header = self.set_json_header()
        self.areaBase = areaBase
        self.catalog = catalog
        
        self.tgtBucket = TGT_BUCKET
        self.tgtRepo = "rawjson"

    def set_json_header(self):

        json_header_template = {"data_type": "{}_unit_data".format(self.unit_type),
                                "target_filename": self.baseName + ".json",
                                "collection_date": self.collection_date}
        return json_header_template

    def to_catalog(self):

        catalog = self.catalog
        pointer = self.tgtStoragePath
        collection_date = self.collection_date
        processing_date = str(date_util.localize(arrow.now().datetime))
        header = self.header
        unit_type = self.unit_type

        metadata = {"repository": self.tgtRepo, "data_source": unit_type,
                    "id_base": self.areaBase, "id_ext": "unit_data.json", "pointer": pointer,
                    "collection_date": collection_date,
                    "processing_date": processing_date, "metadata": header}

        catalog.upsert(metadata)


    def upload_unit_locations(self):

        #call to knack_access.get_device_locations
        json_data = {'header': self.header,
                     'devices': get_device_locations(device_type=self.unit_type,
                                                          app_id=config.KNACK_APP_ID,
                                                          api_key=config.KNACK_API_KEY).create_json()
                        }
        ##write to s3 raw json bucket
        fullPathW = os.path.join(_TEMP_DIR, self.baseName + ".json")
        with open(fullPathW, 'w') as json_file:
            json_file.write(dumps(json_data))

        with open(fullPathW, 'rb') as json_file:
            s3Object = _S3.Object(self.tgtBucket, self.tgtStoragePath)
            s3Object.put(Body=json_file)

        # Clean up:
        os.remove(fullPathW)


def set_S3_pointer(filename, date, data_source='bt'): ### may have to include bucket!! ###

    year = str(date.year)
    month = str(date.month)
    day = str(date.day)

    s_year = year
    s_month = month if len(month) == 2 else month.zfill(2)
    s_day = day if len(day) == 2 else day.zfill(2)

    return "{year}/{month}/{day}/{data_source}/{file}".format(year=s_year,
                                                            month=s_month,
                                                            day=s_day,
                                                            data_source=data_source,
                                                            file=filename)

def insert_units_to_bucket2(areaBase, utype, sameDay=False):
    "Main entry-point that calls class"

    global _TEMP_DIR
    global _S3

    # Catalog and AWS connections:
    catalog = Postgrest(config.CATALOG_URL, auth=config.CATALOG_KEY)
    _S3 = config.getAWSSession().resource('s3')

    # Set up temporary output directory:
    _TEMP_DIR = tempfile.mkdtemp()
    print("Created holding place: %s" % _TEMP_DIR)

    today = date_util.localize(arrow.now().datetime).replace(hour=0, minute=0, second=0, microsecond=0)
    ourDay = today if sameDay else today - datetime.timedelta(days=1) 
    baseName = "{}_unit_data_{}".format(areaBase, ourDay.strftime("%Y-%m-%d"))
    tgtStorage_path = set_S3_pointer(filename=baseName + ".json",
                                         date=ourDay, data_source=utype)
    print("%s:%s" % (TGT_BUCKET, tgtStorage_path))
    worker = unitLocations_toBucket2(unit_type=utype, baseName=baseName,
                                         tgtDate=ourDay, tgtStoragePath=tgtStorage_path,
                                         areaBase=areaBase, catalog=catalog)
    worker.upload_unit_locations()
    worker.to_catalog()

    #Clean  up temporary output directory
    shutil.rmtree(_TEMP_DIR)
