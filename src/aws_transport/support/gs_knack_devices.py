"""
gs_knack_devices.py contains calls into Knack for retrieving details from Knack, as well as support for reading Site files.
"""
import re

from aws_transport.support import config
from aws_transport.support.knack_access import get_device_locations
from collecting.gs import device

class GSKnackDevices:
    "GSKnackDevices handles retrieving of GRIDSMART information from Knack and GRIDSMART site data."
    
    def __init__(self):
        "Initializes."
        
        self.locationEngine = None
        self.validIndices = None
        self.devices = None
        self.siteFiles = None
        self.hardwareInfoFiles = None
        self.streetNames = None
    
    def _retrieveKnack(self):
        "Retrieves device information from Knack and identifies the indices (in validIndices) to valid GRIDSMART devices."
        
        self.locationEngine = get_device_locations("gs", app_id=config.KNACK_APP_ID, api_key=config.KNACK_API_KEY)
        self.validIndices = []
        self.devices = []
        self.siteFiles = []
        self.timeFiles = []
        self.hardwareInfoFiles = []
        self.streetNames = []
        
    def _retrieveDevice(self, index):
        "Constructs device object from GRIDSMART API. Retrieves site file from device."
        
        siteFiles = []
        ourDevice = device.deviceFromAPI(self.locationEngine.device_ips[index].strip(), siteFileRet=siteFiles)
        # Recall that we can get the log reader by calling log_reader.LogReader(device).

        return ourDevice, siteFiles[0], siteFiles[1], siteFiles[2]
        
    def retrieveDevices(self, devFilter=".*"):
        "Takes list of addresses from Knack and gathers site files to build a name->Device dictionary."
        
        self._retrieveKnack()
        regexp = re.compile(devFilter)
        for index, row in self.locationEngine.device_locations.iterrows():
            #if row["ip_comm_status"] == "ONLINE":
            #if True: # TODO: It seems as though the "ip_comm_status" often says "OFFLINE" when the device is actually responding. 
            if row["device_status"].strip().upper() != "REMOVED" and row["atd_location_id"] and str(row["atd_location_id"]) != 'nan':
                streetName = row["primary_st"] + "_" + row["cross_st"]
                if not regexp.search(streetName):
                    continue
                try:
                    ourDevice, siteFile, timeFile, hardwareInfoFile = self._retrieveDevice(index)
                except Exception:
                    print("ERROR: A problem was encountered in accessing Device %s." % self.locationEngine.device_ips[index])
                    continue
                self.devices.append(ourDevice)
                self.siteFiles.append(siteFile)
                self.timeFiles.append(timeFile)
                self.hardwareInfoFiles.append(hardwareInfoFile)
                self.validIndices.append(index)
                self.streetNames.append(streetName)
        return self.devices
    
    def getKnackJSON(self):
        "Returns Knack JSON file contents. Must be called after retrieveDevices()"
        
        return self.locationEngine.create_json()
    
    def getSiteFiles(self):
        "Returns a device->site file contents dictionary. Must be called after retrieveDevices()."
        
        ret = {}
        for index in range(len(self.validIndices)):
            ret[self.devices[index]] = self.siteFiles[index]
        return ret

    def getTimeFiles(self):
        "Returns a device->datetime file contents dictionary. Must be called after retrieveDevices()."
        
        ret = {}
        for index in range(len(self.validIndices)):
            timeFile = {}
            timeFile["DateTime"] = self.timeFiles[index]["DateTime"]
            timeFile["TimeZoneId"] = self.timeFiles[index]["TimeZoneId"]
            timeFile["HostTimeUTC"] = self.timeFiles[index]["HostTimeUTC"]
            ret[self.devices[index]] = timeFile
        return ret
    
    def getHardwareInfoFiles(self):
        "Returns a device->hardwareinfo contents dictionary. Must be called after retrieveDevices()."
        
        ret = {}
        for index in range(len(self.validIndices)):
            ret[self.devices[index]] = self.hardwareInfoFiles[index]
        return ret
    
    def getAllFiles(self):
        "Returns a dictionary of dictionaries that contain site, time, and hardware info files."
        
        ret = {}
        siteFiles = self.getSiteFiles()
        timeFiles = self.getTimeFiles()
        hardwareInfoFiles = self.getHardwareInfoFiles()
        
        for key in siteFiles:
            ret[key] = {"site": siteFiles[key],
                        "time": timeFiles[key],
                        "hardware_info": hardwareInfoFiles[key]}
        return ret
