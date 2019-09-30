'''
Searches through Knack and returns hardware IDs for each GRIDSMART device that's found.
'''
import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter

import _setpath
from aws_transport.support import last_upd_gs

PROGRAM_DESC = "Searches through Knack and returns hardware IDs for each GRIDSMART device that's found."

def main():
    # Parse command-line parameter:
    parser = ArgumentParser(description=PROGRAM_DESC, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-o", "--output", default="", help="Specifies output for hardware IDs")
    parser.add_argument("-f", "--devname_filter", default=".*", help="filter processing on units whose street names match the given regexp")
    
    # TODO: Consider parameters for writing out files?
    args = parser.parse_args()

    # Construct list of all GRIDSMART devices and log readers:
    _, allFiles, knackJSON = last_upd_gs.getDevicesLogreaders(devFilter=args.devname_filter)
    
    # Output ATD ID and hardware identifiers:
    fileOut = sys.stdout
    if args.output:
        fileOut = open(args.output, "w")

    print("atd_device_id,gs_hardware_id", file=fileOut)
    deviceLookup = {device.netAddr: device for device in allFiles}
    for knackEntry in knackJSON:
        if knackEntry["device_ip"] in deviceLookup:
            device = deviceLookup[knackEntry["device_ip"]]
            hardwareEntry = allFiles[device]["hardware_info"]
            # TODO: If we want to have serial number in there, we can do: hardwareEntry["HardwareId"] + "_" + hardwareEntry["BoxSerialNumber"]
            print("%d,%s" % (knackEntry["atd_device_id"], hardwareEntry["HardwareId"]), file=fileOut)
            
    if args.output:
        fileOut.close()

if __name__ == "__main__":
    main()
