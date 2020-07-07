"""
unitdata_knack_common.py contains common functions used in building up unit data.

@author Kenneth Perrine, Nadia Florez
"""
import pandas as pd
from knackpy import Knack

from support import unitdata

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
             'PRIMARY_ST_SEGMENT_ID': 'primary_st_segment_id'}

class UnitDataCommonKnack:
    """
    Common functionality for gathering unit data from Knack
    """
    def __init__(self, device, devFilter, appID, apiKey, areaBase, sameDay=False):
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

    def getLocations(self):
        """
        Obtain the ATD Locations Knack table
        """
        locsAccessor = Knack(obj='object_11', app_id=self.appID, api_key=self.apiKey)
    
        atdLocColumns = ['ATD_LOCATION_ID', 'COA_INTERSECTION_ID', 'CROSS_ST',
               'CROSS_ST_SEGMENT_ID','LOCATION_latitude', 'LOCATION_longitude',
               'PRIMARY_ST', 'PRIMARY_ST_SEGMENT_ID', 'SIGNAL_ID']
    
        return pd.DataFrame(locsAccessor.data)[atdLocColumns]
    
    def getDevices(self):
        """
        Obtain filtered information from the Knack devices table
        """
        device_filters = {'match': 'and',
                          'rules': [
                                    {
                                     'field': 'field_884',
                                     'operator': 'is',
                                     'value': self.devFilter
                                     }]}
    
        device_locs = Knack(
                       obj='object_56',
                       app_id=self.appID,
                       api_key=self.apiKey,
                       filters=device_filters)
    
        devices_data = pd.DataFrame(device_locs.data)
        devices_data = (pd.merge(devices_data, self.getLocations(),
                                 on='ATD_LOCATION_ID', how='left')
                        .drop(labels='SIGNAL_ID', axis='columns')
                        .rename(columns=TS_RENAME))
        # Reorder the columns:
        devices_data = devices_data[['device_type', 'atd_device_id',
                                     'device_name', 'device_status', 'device_ip',
                                     'ip_comm_status', 'atd_location_id',
                                     'coa_intersection_id',
                                     'lat', 'lon', 'primary_st',
                                     'primary_st_segment_id',
                                     'cross_st', 'cross_st_segment_id']]
    
        return devices_data

    def retrieve(self):
        """
        This retrieves a unit data dictionary for this data type.
        """
        devices = self.getDevices().create_json()
        header = unitdata.makeHeader(self.areaBase, self.device, self.sameDay)
        jsonData = {'header': header,
                    'devices': devices}
        return jsonData

    def store(self, unitData=None):
        """
        This stores a unit data JSON files for this data type.
        """
        raise Exception("unitdata_knack_common: Storage is not supported.")
