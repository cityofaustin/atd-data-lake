"""
bt_unitdata_knack.py contains Knack-based Unit Data accessor for Bluetooth

@author Kenneth Perrine
"""
from drivers.devices.unitdata_knack_common import UnitDataCommonKnack

class BTUnitDataKnack(UnitDataCommonKnack):
    """
    Handles Bluetooth-specific location information access from Knack.
    """
    def __init__(self, appID, apiKey):
        """
        Initializes the object.
        
        @param appID is the Knack app ID to use for Knack access
        @param apiKey is the Knack API key used for Knack access
        """
        super().__init__("BLUETOOTH", appID, apiKey)
        