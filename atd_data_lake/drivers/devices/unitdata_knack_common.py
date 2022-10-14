"""
unitdata_knack_common.py contains common functions used in building up unit data.

@author Kenneth Perrine, Nadia Florez
"""
import math, numbers

import pandas as pd
import knackpy

from atd_data_lake.support import unitdata

TS_RENAME = {'ATD_LOCATION_ID': 'atd_location_id',
             'ATD_SENSOR_ID': 'atd_device_id',
             'READER_ID': 'device_name',
             'SENSOR_IP': 'device_ip',
             'IP_COMM_STATUS': 'ip_comm_status',
             'SENSOR_TYPE': 'device_type',
             'SENSOR_STATUS': 'device_status',
             'COA_INTERSECTION_ID': 'coa_intersection_id',
             'CROSS_ST': 'cross_st',
             'CROSS_ST_SEGMENT_ID': 'cross_st_segment_id',
             'LOCATION_latitude': 'lat',
             'LOCATION_longitude': 'lon',
             'PRIMARY_ST': 'primary_st',
             'PRIMARY_ST_SEGMENT_ID': 'primary_st_segment_id',
             'KITS_ID': 'kits_id'}

class UnitDataCommonKnack:
    """
    Common functionality for gathering unit data from Knack
    """
    def __init__(self, device, devFilter, appID, apiKey, areaBase, sameDay=True):
        """
        Sets parameters for object operation
        
        @param appID is the Knack app ID to use for Knack access
        @param apiKey is the Knack API key used for Knack access
        @param device is the device key to add into the header for the unit data.
        @param devFilter is used in filtering for the specific device type when querying Knack
        """
        self.appID = appID
        self.apiKey = apiKey
        self.device = device
        self.devFilter = devFilter
        self.areaBase = areaBase
        self.sameDay = sameDay
        self.locations = None

    def _getLocations(self):
        """
        Obtain the ATD Locations Knack table
        """
        knackApp = knackpy.App(app_id=self.appID, api_key=self.apiKey)
        locsAccessor = knackApp.get('object_11',
                                    generate=True)
        locs = []
        for loc in locsAccessor:
            rec = loc.format(values=False)
            if 'LOCATIONS' in rec:
                rec['LOCATION_latitude'] = rec['LOCATION']['latitude']
                rec['LOCATION_longitude'] = rec['LOCATION']['longitude']
            else:
                rec['LOCATION_latitude'] = None
                rec['LOCATION_longitude'] = None
            locs.append(rec)
        del knackApp, locsAccessor
    
        atdLocColumns = ['ATD_LOCATION_ID', 'COA_INTERSECTION_ID', 'CROSS_ST',
               'CROSS_ST_SEGMENT_ID', 'LOCATION_latitude', 'LOCATION_longitude',
               'PRIMARY_ST', 'PRIMARY_ST_SEGMENT_ID', 'SIGNAL_ID']
    
        self.locations = pd.DataFrame(locs)[atdLocColumns]
        return self.locations
    
    def getDevices(self):
        """
        Obtain filtered information from the Knack devices table
        """
        deviceFilters = {'match': 'and',
                         'rules': [{'field': 'field_884',
                                    'operator': 'is',
                                    'value': self.devFilter}]}
        knackApp = knackpy.App(app_id=self.appID, api_key=self.apiKey)
        deviceLocs = knackApp.get('object_56',
                                  filters=deviceFilters,
                                  generate=True)
        deviceLocs = [loc.format() for loc in deviceLocs]

        devicesData = pd.DataFrame(deviceLocs)
        devicesData = (pd.merge(devicesData, self._getLocations(),
                                on='ATD_LOCATION_ID', how='left')
                        .drop(labels='SIGNAL_ID', axis='columns')
                        .rename(columns=TS_RENAME))
        # Reorder the columns:
        devicesData = devicesData[['device_type', 'atd_device_id',
                                   'device_name', 'device_status', 'device_ip',
                                   'ip_comm_status', 'atd_location_id',
                                   'coa_intersection_id',
                                   'lat', 'lon', 'primary_st',
                                   'primary_st_segment_id',
                                   'cross_st', 'cross_st_segment_id',
                                   'kits_id']]
        return devicesData

    def retrieve(self):
        """
        This retrieves a unit data dictionary for this data type.
        """
        print("Retrieving Unit Data...")
        devices = self.getDevices().to_dict(orient="records")
        for device in devices:
            if 'kits_id' in device:
                device['kits_id'] = cInt(device['kits_id'])
            device['lat'] = cFlt(device['lat'])
            device['lon'] = cFlt(device['lon'])
            device['coa_intersection_id'] = cInt(device['coa_intersection_id'])
            device['primary_st'] = tStr(device['primary_st'])
            device['primary_st_segment_id'] = cInt(device['primary_st_segment_id'])
            device['cross_st'] = tStr(device['cross_st'])
            device['cross_st_segment_id'] = cInt(device['cross_st_segment_id'])
        header = unitdata.makeHeader(self.areaBase, self.device, self.sameDay)
        jsonData = {'header': header,
                    'devices': devices}
        return jsonData

    def store(self, unitData=None):
        """
        This stores a unit data JSON files for this data type.
        """
        raise NotImplementedError("unitdata_knack_common: Storage is not supported.")

def cInt(val):
    """
    Converts to integer if not None or NaN, otherwise returns None.
    """
    try:
        return int(float(val)) if not (val is None or isinstance(val, numbers.Number) and math.isnan(val)) else None
    except:
        return None

def cFlt(val):
    """
    Converts to integer if not None or NaN, otherwise returns None.
    """
    try:
        return float(val) if not (val is None or isinstance(val, numbers.Number) and math.isnan(val)) else None
    except:
        return None

def tStr(val):
    """
    Trims the string if not None, otherwise returns None.
    """
    try:
        return str(val).strip() if val is not None else None
    except:
        return None
    
