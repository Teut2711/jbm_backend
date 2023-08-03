from datetime import datetime
from enum import Enum
import random
import json
import string
import time
import uuid

from datetime import datetime, timedelta
from geopy.geocoders import Nominatim


class SWITCH:
    ON = "on"
    OFF = "off"


def get_address_from_lat_long(latitude, longitude):
    geolocator = Nominatim(user_agent="myGeocoder")
    location = geolocator.reverse(f"{latitude}, {longitude}", exactly_one=True)

    if location is not None:
        address = location.address
        print(address)
        return address
    else:
        return ""


def random_datetime(start_date, end_date):
    # Calculate the time range in seconds
    time_range = (end_date - start_date).total_seconds()

    # Generate a random number of seconds within the time range
    random_seconds = random.randint(0, int(time_range))

    # Create the random datetime within the interval
    random_datetime = start_date + timedelta(seconds=random_seconds)

    return random_datetime.isoformat()


end_date = datetime.now()
start_date = datetime(2022, 10, 1, 23, 59, 59)


dummy_buses_data = []
dummy_faults_data = []

dummy_specific_bus_data = []
dummy_specific_fault_data = []
fault_description = [
    {
        "code": f"F{random.randint(125, 400)}",
        "label": "B2V CellVoltTooHigh",
    },
    {
        "code": f"F{random.randint(125, 400)}",
        "label": "B2V BatTempTooHigh",
    },
    {
        "code": f"F{random.randint(125, 400)}",
        "label": "B2V BMSWorkVoltError",
    },
    {
        "code": f"F{random.randint(125, 400)}",
        "label": "B2V TempDiff",
    },
    {
        "code": f"F{random.randint(125, 400)}",
        "label": "B2V TempNotControl",
    },
]

for i in range(50):
    time.sleep(5)
    bus_status = random.choice(
        [
            "in-depot",
            "in-field",
            "charging",
            "discharging",
            "disconnected",
            "full-charged",
            "in-fault",
            "idle",
        ]
    )
    depot_number = f"Depot {random.choice(string.ascii_uppercase)}"
    latitude = round(
        random.uniform(28.0, 28.9), 6
    )  # Random latitude in the range 28.0 to 28.9
    longitude = round(
        random.uniform(76.0, 77.2), 6
    )  # Random longitude in the range 76.0 to 77.2
    _uuid = str(uuid.uuid4())
    _imei = str(random.randint(10**14, 10**15))
    _battery_number = random.randint(0, 100)

    bus_data = {
        "uuid": _uuid,
        "depotNumber": depot_number,
        "busNumber": f"Bus-00{i+1}",
        "IMEI": _imei,
        "battery": f"BAT{_battery_number}",
        "status": bus_status,
        "coordinates": {"lat": latitude, "lng": longitude},
    }
    specific_bus_data = {
        "uuid": _uuid,
        "busNumber": f"Bus-00{i+1}",
        "timestamp": random_datetime(start_date, end_date),
        "IMEI": _imei,
        "location": {
            "address": get_address_from_lat_long(latitude, longitude),
            "coordinates": {"lat": latitude, "lng": longitude},
        },
        "totalAlerts": random.randint(1, 10),
        "statusOptions": {
            "bus": {"text": "Online", "status": "on"},  # on , off
            "CANData": {"text": "CAN Data", "status": "on"},
            "externalPower": {"text": "External Power", "status": "on"},
            "deviceData": {"text": "Device Data", "status": "on"},
            "GPSData": {"text": "GPS Data", "status": "on"},
            "busRunning": {"text": "Bus Running", "status": "on"},
        },
        "batteryOverview": {
            "soc": {
                "text": "SoC",
                "value": round(random.random() * 100, 2),
                "units": "%",
            },
            "soh": {
                "text": "SoH",
                "value": round(random.random() * 100, 2),
                "units": "%",
            },
            "temperature": {
                "text": "Temperature",
                "value": round(25 + 75 * random.random(), 2),
                "units": "°C",
            },
            "voltage": {
                "text": "Voltage",
                "value": random.randint(200, 500),
                "units": "V",
            },
            "current": {
                "text": "Current",
                "value": random.randint(50, 100),
                "units": "A",
            },
            "regenation": {
                "text": "Regeneration",
                "value": "Disabled",
            },
            "BMSStatus": {
                "text": "BMS Status",
                "value": "Normal",
            },
            "speed": {
                "text": "Speed",
                "value": random.randint(50, 100),
                "units": "km/h",
            },
            "contractorStatus": {
                "text": "String Contractor Status",
                "value": "Closed",
            },
            "cellVoltageDelta": {
                "text": "String-Wise Delta of Cell Voltage",
                "min": round(random.uniform(0, 0.5), 2),
                "max": round(random.uniform(0.5, 1), 2),
                "units": "V",
            },
            "temperatureDelta": {
                "text": "String-Wise Delta of Temperature",
                "min": random.randint(25, 35),
                "max": round(random.uniform(50, 100), 2),
                "units": "°C",
            },
        },
    }
    _uuid = str(uuid.uuid4())
    _random_fault_description = random.choice(fault_description)
    _fault_time = str(random_datetime(start_date, end_date))
    fault_data = {
        "status": bus_status,
        "uuid": _uuid,
        "faultCode": _random_fault_description["code"],
        "faultDescription": _random_fault_description["label"],
        "faultTime": _fault_time,
        "faultDuration": f"{random.randint(1, 10)} hours ago",
    }
    specific_fault_data = {
        "uuid": _uuid,
        "faultCode": _random_fault_description["code"],
        "faultTime": _fault_time,
        "faultDescription": _random_fault_description["label"],
        "faultDuration": f"{random.randint(1, 10)} hours ago",
        "busNumber": f"Bus-00{i+1}",
        "battery": f"BAT{_battery_number}",
        "IMEI": _imei,
        "depotNumber": depot_number,
        "faultLevel": "off",
        "location": {
            "coordinates": {"lat": latitude, "lng": longitude},
        },
    }

    dummy_faults_data.append(fault_data)
    dummy_buses_data.append({**bus_data, **specific_bus_data})
    dummy_specific_bus_data.append(specific_bus_data)
    dummy_specific_fault_data.append(specific_fault_data)


with open("./busData.json", "w") as file:
    json.dump(dummy_buses_data, file, indent=2)

with open("./faultData.json", "w") as file:
    json.dump(dummy_faults_data, file, indent=2)


with open("./specificBusData.json", "w") as file:
    json.dump(dummy_specific_bus_data, file, indent=2)


with open("./specificFaultData.json", "w") as file:
    json.dump(dummy_specific_fault_data, file, indent=2)


print("Data saved")
