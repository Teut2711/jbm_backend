from geopy.geocoders import Nominatim


class SWITCH:
    ON = "on"
    OFF = "off"


def get_address_from_lat_long(latitude, longitude):
    geolocator = Nominatim(user_agent="myGeocoder")
    location = geolocator.reverse(f"{latitude}, {longitude}", exactly_one=True)

    if location is not None:
        address = location.address
        return address
    else:
        return ""

   