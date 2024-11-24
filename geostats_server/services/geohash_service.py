import geohash

def get_geohash(lat, lng, precision=6):
    """
    Generate a geohash for the given latitude and longitude.
    """
    return geohash.encode(lat, lng, precision)
