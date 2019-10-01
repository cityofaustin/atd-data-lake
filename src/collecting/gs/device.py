'''
device.py contains a class that represents a GRIDSMART device.

@author: Kenneth Perrine
'''

from __future__ import print_function
import requests
import datetime
import sys

URL_PROTO = "http"
URL_PORT = 8902

class Device:
    """
    Device represents a GRIDSMART device
    """
    def __init__(self):
        """
        Constructor
        """
        self.street1 = ""
        self.street2 = ""
        self.lat = 0.0
        self.lon = 0.0
        self.camID = ""
        self.netAddr = ""
        self.movements = {} # Keyed by GUID.
        
    def getURL(self):
        """
        Returns the start of the URL string for the device.
        """
        return "%s://%s:%d/api/" % (URL_PROTO, self.netAddr, URL_PORT)

class Movement:
    """
    Movement represents a movement or zone within a GRIDSMART device.
    """
    def __init__(self, device):
        """
        Constructor
        """
        self.device = device
        self.guid = ""
        self.zoneName = ""
        self.zoneApproach = ""

def deviceFromAPI(netAddr, siteFileRet=None):
    """
    Uses the GRIDSMART API to populate a Device object.
    
    @param netAddr IP address of GRIDSMART device
    @param siteFileRet Pass in an empty array; if defined, the site file contents is stored in [0] as JSON,
            the datetime file contents is stored in [1] as JSON, and hardware info file contents is stored as [2] as JSON.
    """ 
    baseURL = "%s://%s:%d/api/" % (URL_PROTO, netAddr, URL_PORT)
    
    print("-- %s --" % netAddr, file=sys.stderr)
    try:
        siteResponse = requests.get(baseURL + "site.json", timeout=15)
        if isinstance(siteFileRet, list):
            timeResponse = requests.get(baseURL + "datetime.json", timeout=15)
            hardwareInfoResponse = requests.get(baseURL + "system/hardwareinfo.json", timeout=15)
    except:
        print("Problem base URL: %s" % baseURL, file=sys.stderr)
        raise
        
    jr = siteResponse.json()
    if isinstance(siteFileRet, list):
        siteFileRet.append(jr)
        timeFile = timeResponse.json()
        timeFile["HostTimeUTC"] = datetime.datetime.utcnow().strftime("%m/%d/%Y %I:%M:%S %p")
        siteFileRet.append(timeFile)
        hardwareInfoFile = hardwareInfoResponse.json()
        siteFileRet.append(hardwareInfoFile)
    return deviceFromJSON(jr, netAddr)
    
def deviceFromJSON(jr, netAddr):
    """
    Uses JSON retrieved from the GRIDSMART API to return a Device object.
    """
    device = Device()
    try:
        device.street1 = jr["Location"]["Street1"]
        device.street2 = jr["Location"]["Street2"]
        device.lat = jr["Location"]["Latitude"]
        device.lon = jr["Location"]["Longitude"]
        camDev = jr["CameraDevices"]
        if len(camDev) == 0:
            print("No camera devices!", file=sys.stderr)
        else:
            if len(camDev) > 1:
                print("WARNING: More than one camera device is present!")
            mac = camDev[0]["Fisheye"]["MACAddress"]
            print("MAC: %s" % mac)
            device.camID = mac.replace(':', '-')
            device.netAddr = netAddr
            
            zones = camDev[0]["Fisheye"]["CameraMasks"]["ZoneMasks"]
            for zone in zones:
                if "Vehicle" not in zone:
                    print("Zone contains non-vehicle.", file=sys.stderr)
                else:
                    movement = Movement(device)
                    vehicle = zone["Vehicle"]
                    movement.guid = vehicle["Id"]
                    
                    # Must insert dashes in right places...
                    if movement.guid.count('-') == 0:
                        movement.guid = movement.guid[:8] + "-" + movement.guid[8:12] + "-" + movement.guid[12:16] + "-" + movement.guid[16:20] + "-" + movement.guid[20:]
                    
                    movement.zoneName = vehicle["TurnType"] # Or Name?
                    movement.zoneApproach = vehicle["ApproachType"]
                    device.movements[movement.guid] = movement
    except NameError:
        raise
        
    return device 
    
