"""
wt_unitdata_knack.py contains Knack-based Unit Data accessor for Wavetronix

@author Kenneth Perrine
"""
from drivers.devices.unitdata_knack_common import UnitDataCommonKnack

class WTUnitDataKnack(UnitDataCommonKnack):
    """
    Handles Wavetronix-specific location information access from Knack.
    """
    def __init__(self, appID, apiKey, areaBase):
        """
        Initializes the object.
        
        @param appID is the Knack app ID to use for Knack access
        @param apiKey is the Knack API key used for Knack access
        """
        super().__init__("wt", "RADAR", appID, apiKey, areaBase)
