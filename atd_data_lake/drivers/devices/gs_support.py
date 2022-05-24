"""
gs_support.py: Support routines for GRIDSMART

@author Kenneth Perrine and Nadia Florez
"""
import collections
import re

from drivers.devices import gs_device
from drivers.devices import gs_log_reader

"Return type for the getDevicesLogreaders() function:"
_GSDeviceLogreader = collections.namedtuple("_GSDeviceLogreader", "device logReader site timeFile hwInfo streetNames")

def getDevicesLogreaders(gsUnitData, devFilter=".*"):
    """
    Attempts to retrieve all devices and log readers using the gs_intersections table. Devices that can't be contacted
    are not added to the list.
    
    @return List of _GSDeviceLogreader objects.
    """
    # Get the devices:
    devices = retrieveDevices(gsUnitData, devFilter)
    
    # Get counts availability for all of these devices:
    ret = []
    count = 0
    errs = 0
    print("== Collecting device availability ==")
    for index, deviceContainer in enumerate(devices):
        print("Device: %d: %s_%s... " % (index, deviceContainer.device.street1, deviceContainer.device.street2), end='')
        try:
            logReader = gs_log_reader.LogReader(deviceContainer.device)
            ret.append(_GSDeviceLogreader(device=deviceContainer.device,
                                          logReader=logReader,
                                          site=deviceContainer.site,
                                          timeFile=deviceContainer.timeFile,
                                          hwInfo=deviceContainer.hwInfo,
                                          streetNames=deviceContainer.streetNames))
            count += 1
            print("OK")
        except Exception as exc:
            print("ERROR: A problem was encountered in accessing.") 
            print(exc)
            errs += 1
    print("Result: Sucesses: %d; Failures: %d" % (count, errs))
    return ret

def _retrieveDevice(deviceIP):
    "Constructs device object from GRIDSMART API. Retrieves site file and other information from device."
    
    siteFiles = []
    ourDevice = gs_device.deviceFromAPI(deviceIP, siteFileRet=siteFiles)
    # Recall that we can get the log reader by calling log_reader.LogReader(device).

    return ourDevice, siteFiles[0], siteFiles[1], siteFiles[2]

"Return type for the retrieveDevices() function:"
_GSDevice = collections.namedtuple("_GSDevice", "device site timeFile hwInfo streetNames")

def retrieveDevices(gsUnitData, devFilter=".*"):
    """
    Takes list of addresses from Knack and gathers site files to build a list of _GSDevice objects.
    
    @return List of _GSDevice objects.
    """
    ret = []
    ips = set() # To prevent duplicates
    regexp = re.compile(devFilter)
    for row in gsUnitData["devices"]:
        #if row["ip_comm_status"] == "ONLINE":
        #if True: # TODO: It seems as though the "ip_comm_status" often says "OFFLINE" when the device is actually responding. 
        if row["device_status"].strip().upper() != "REMOVED" and row["atd_location_id"] and str(row["atd_location_id"]) != 'nan':
            if not row["device_ip"]:
                print("WARNING: Device %s has no 'device_ip' defined." % str(row["atd_location_id"]))
                continue
            key = row["device_ip"].strip().lower()
            if key in ips:
                print("WARNING: Device address '%s' is duplicated. Skipping." % row["device_ip"])
                continue
            ips.add(key)
            
            streetName = (row["primary_st"] if row["primary_st"] else "") + "_" + (row["cross_st"] if row["cross_st"] else "")
            streetName = streetName.replace("/", "&") # Needed to sanitize for filenames.
            if not regexp.search(streetName):
                continue
            try:
                ourDevice, siteFile, timeFile, hardwareInfoFile = _retrieveDevice(row["device_ip"])
                timeFile = {"DateTime": timeFile["DateTime"],
                            "TimeZoneId": timeFile["TimeZoneId"],
                            "HostTimeUTC": timeFile["HostTimeUTC"]}
                ret.append(_GSDevice(device=ourDevice,
                                     site=siteFile,
                                     timeFile=timeFile,
                                     hwInfo=hardwareInfoFile,
                                     streetNames=streetName))
            except Exception:
                print("ERROR: A problem was encountered in accessing Device %s." % row["device_ip"])
                continue
    return ret
