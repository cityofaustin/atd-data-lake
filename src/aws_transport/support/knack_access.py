'''Device information access from Knack for gs, bt and wt
Author: Nadia Florez'''

import pandas as pd
from knackpy import Knack

class get_device_locations:

    '''Class returns table with device information for bt, wt or gs,
    and returns device location data as a json structure.
    Calling for gs information also returns IP addresses.'''

    def __init__ (self, device_type, app_id, api_key):

        self.device_type = device_type
        self.app_id = app_id
        self.api_key = api_key
        self.atd_locations = self.atd_locations()
        self.device_locations = self.device_locations()
        self.device_ips = self.get_ips()
        self.locations_json = self.create_json()

    def atd_locations(self):

        atd_locs = Knack(
                       obj='object_11',
                       app_id=self.app_id,
                       api_key=self.api_key)

        atd_loc_columns = ['ATD_LOCATION_ID', 'COA_INTERSECTION_ID', 'CROSS_ST',
               'CROSS_ST_SEGMENT_ID','LOCATION_latitude', 'LOCATION_longitude',
               'PRIMARY_ST', 'PRIMARY_ST_SEGMENT_ID', 'SIGNAL_ID']

        return pd.DataFrame(atd_locs.data)[atd_loc_columns]

    def device_locations(self):



        ts_rename = {'ATD_LOCATION_ID': 'atd_location_id',
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

        gs_rename = {'DETECTOR_ID': 'atd_device_id',
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

        if self.device_type == 'gs':

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
                            .rename(columns=gs_rename))
            devices_data = devices_data[['device_type', 'atd_device_id',
                                         'device_ip', 'device_status',
                                         'ip_comm_status', 'atd_location_id',
                                         'coa_intersection_id',
                                         'lat', 'lon', 'primary_st',
                                         'primary_st_segment_id',
                                         'cross_st', 'cross_st_segment_id']]

        elif self.device_type == 'bt':

            device_filters = {'match': 'and',
                              'rules': [
                                        {
                                         'field': 'field_884',
                                         'operator': 'is',
                                         'value': 'BLUETOOTH'
                                         }]}

            device_locs = Knack(
                           obj='object_56',
                           app_id=self.app_id,
                           api_key=self.api_key,
                           filters=device_filters)

            devices_data = pd.DataFrame(device_locs.data)
            devices_data = (pd.merge(devices_data, self.atd_locations,
                                     on='ATD_LOCATION_ID', how='left')
                            .drop(labels='SIGNAL_ID', axis='columns')
                            .rename(columns=ts_rename))
            ##re-ordering columns
            devices_data = devices_data[['device_type', 'atd_device_id',
                                         'device_name', 'device_status', 'device_ip',
                                         'ip_comm_status', 'atd_location_id',
                                         'coa_intersection_id',
                                         'lat', 'lon', 'primary_st',
                                         'primary_st_segment_id',
                                         'cross_st', 'cross_st_segment_id']]

        elif self.device_type == 'wt':

            device_filters = {'match': 'and',
                              'rules': [
                                        {
                                         'field': 'field_884',
                                         'operator': 'is',
                                         'value': 'RADAR'
                                         }]}

            device_locs = Knack(
                           obj='object_56',
                           app_id=self.app_id,
                           api_key=self.api_key,
                           filters=device_filters)

            devices_data = pd.DataFrame(device_locs.data)
            devices_data = (pd.merge(devices_data, self.atd_locations,
                                     on='ATD_LOCATION_ID', how='left')
                            .drop(labels='SIGNAL_ID', axis='columns')
                            .rename(columns=ts_rename))
            devices_data = devices_data[['device_type', 'atd_device_id',
                                         'device_name', 'device_status', 'device_ip',
                                         'ip_comm_status', 'atd_location_id',
                                         'coa_intersection_id',
                                         'lat', 'lon', 'primary_st',
                                         'primary_st_segment_id',
                                         'cross_st', 'cross_st_segment_id']]

        else:
            print("Parameter device_type has to be on of 'bt', 'wt', or 'gs'")
            devices_data = None
            ##throw error

        return devices_data

    def get_ips(self):

        try:
            return self.device_locations.device_ip.tolist()

        except Exception as e:
            print(e)
            return None

    def create_json(self):

        try:
            return self.device_locations.apply(lambda row: row.to_dict(), axis=1).tolist()
        except Exception as e:
            print(e)
            return None
