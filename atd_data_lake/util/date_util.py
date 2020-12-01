"""
date_util.py: Utilities for dealing with dates

Kenneth Perrine - The University of Texas at Austin
"""

import arrow
import datetime as dt
from dateutil.tz import tzutc
import pytz

"""
Local timezone must be set via setLocalTimezone() before methods can be used in this module.

TODO: Use the tzlocal package to get the local timezone in pytz form so localize() can be called on it.
"""
LOCAL_TIMEZONE = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo

"""
EARLIEST_TIME is the default time that is provided as a default from parseDate when 0 is given.
"""
EARLIEST_TIME = dt.datetime(2000, 1, 1, tzinfo=LOCAL_TIMEZONE)

def parseDate(dateString, dateOnly=False):
    """
    Uses best guess to parse incoming dateString. If dateString is None or "0", then the EARLIEST_TIME will be returned.
    If no time zone information is provided, then the time zone will be set to the local time zone.
    
    @param dateString Use a string in a parseable form, preferably YYYY-MM-DD, or a datetime object.
    @param dateOnly Set this to True to truncate the given date to midnight.
    
    TODO: Use a better date/time parser that can tell the difference between UTC and no time zone.
    """
    
    if dateString:
        if isinstance(dateString, str):
            dateString = dateString.strip()
        if dateString != "0":
            try:
                # Use best-guess to parse the date:
                aRunDate = arrow.get(dateString)
            except arrow.parser.ParserError:
                # Try UNIX format:
                aRunDate = arrow.Arrow.fromtimestamp(dateString)
            
            # Check to see if the provided time was explicitly expressed as UTC:
            explicitUTC = False
            if isinstance(dateString, str) and dateString[-3:] == "UTC" \
                    or isinstance(dateString, dt.datetime) and isinstance(dateString.tzinfo, tzutc):
                explicitUTC = True
            
            # If UTC had been assumed, but wasn't found explicit, then likely no time zone was specified.
            # We'll default to our local time zone.
            ourDate = aRunDate.datetime
            if not explicitUTC and isinstance(aRunDate.tzinfo, tzutc):
                ourDate = localOverwrite(ourDate)
                
            if dateOnly:
                ourDate = ourDate.replace(hour=0, minute=0, second=0, microsecond=0)
                
            # Assign to what we'll be using:
            return ourDate
        
    # Default our last run time to an earliest possible time:
    return EARLIEST_TIME

def localize(dateTime):
    """
    Translates the given dateTime to local time. If no time zone information is given, then local time zone will be applied.
    
    TODO: Replace this material with Arrow... it will be a lot cleaner
    """
    # Try to convert the time to naive because pytz can't localize a datetime that has another time zone.
    if not dateTime:
        return dateTime
    if dateTime.tzinfo is None or dateTime.tzinfo.utcoffset(dateTime) is None:
        return LOCAL_TIMEZONE.localize(dateTime)
    else:    
        return dateTime.astimezone(LOCAL_TIMEZONE)    
    
def localOverwrite(dateTime):
    """
    Overwrites timezone information (or naivete) to local time.
    """
    return localize(dateTime.replace(tzinfo=None))

def setLocalTimezone(timeZoneString):
    """
    Sets the module-level time zone so that the localize methods will work correctly.
    """
    global LOCAL_TIMEZONE
    if timeZoneString:
        LOCAL_TIMEZONE = pytz.timezone(timeZoneString)

def roundDay(timestampIn):
    """
    Returns a version of the timestamp where time is zeroed to midnight.
    """
    return timestampIn.replace(hour=0, minute=0, second=0, microsecond=0)

def getNow(dayOnly=False):
    """
    Returns the local current time. If dayOnly is true, rounds the timestamp to the start of the day.
    """
    now = localize(arrow.now().datetime)
    if dayOnly:
        now = roundDay(now)
    return now
