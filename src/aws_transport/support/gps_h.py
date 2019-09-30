"""
gps_h.py contains a Haversine distance calculator for GPS coordinates.
Lifted from: https://nathanrooy.github.io/posts/2016-09-07/haversine-with-python/
"""
import math

R=6371000                               # radius of Earth in meters

def gps2feet(lat1, lon1, lat2, lon2):
    phi_1 = math.radians(lat1)
    phi_2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0)**2 + \
       math.cos(phi_1) * math.cos(phi_2) * \
       math.sin(delta_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    meters = R * c
    miles = meters * 0.000621371
    feet = miles * 5280
    return feet
