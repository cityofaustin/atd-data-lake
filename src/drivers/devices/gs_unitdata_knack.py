"""
gs_unitdata_knack.py contains Knack-based Unit Data accessor for GRIDSMART

@author Kenneth Perrine
"""
from drivers.devices.unitdata_knack_common import UnitDataCommonKnack

import pandas as pd
from knackpy import Knack

GS_RENAME = {'DETECTOR_ID': 'atd_device_id',
            'DETECTOR_IP': 'device_ip',
            'DETECTOR_STATUS': 'device_status',
            'IP_COMM_STATUS': 'ip_comm_status',
            'SENSOR_TYPE': 'device_type',
            'ATD_LOCATION_ID': 'atd_location_id',
            'COA_INTERSECTION_ID': 'coa_intersection_id',
            'CROSS_ST': 'cross_st',
            'CROSS_ST_SEGMENT_ID': 'cross_st_segment_id',
            'LOCATION_latitude': 'lat',
            'LOCATION_longitude': 'lon',
            'PRIMARY_ST': 'primary_st',
            'PRIMARY_ST_SEGMENT_ID': 'primary_st_segment_id'}

class GSUnitDataKnack(UnitDataCommonKnack):
    """
    Handles GRIDSMART-specific location information access from Knack.
    """
    def __init__(self, appID, apiKey, areaBase):
        """
        Initializes the object.
        
        @param appID is the Knack app ID to use for Knack access
        @param apiKey is the Knack API key used for Knack access
        """
        super().__init__("gs", "GRIDSMART", appID, apiKey, areaBase)
        
    def getDevices(self):
        """
        Calls Knack to retrieve Unit Data.
        """
        device_filters = {'match': 'and',
                          'rules': [
                                    {
                                     'field': 'field_2384',
                                     'operator': 'is',
                                     'value': 1
                                     }]}

        device_locs = Knack(
                       obj='object_98',
                       app_id=self.app_id,
                       api_key=self.api_key,
                       filters=device_filters)

        devices_data = pd.DataFrame(device_locs.data)
        devices_data['SENSOR_TYPE'] = 'GRIDSMART'
        devices_data = (pd.merge(devices_data, self.atd_locations,
                                 on='SIGNAL_ID', how='left')
                        .drop(labels='SIGNAL_ID', axis='columns')
                        .rename(columns=GS_RENAME))
        devices_data = devices_data[['device_type', 'atd_device_id',
                                     'device_ip', 'device_status',
                                     'ip_comm_status', 'atd_location_id',
                                     'coa_intersection_id',
                                     'lat', 'lon', 'primary_st',
                                     'primary_st_segment_id',
                                     'cross_st', 'cross_st_segment_id']]

        return devices_data