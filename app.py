from collections import Counter
import json
from flask import jsonify, request
from flask_cors import CORS
from . import utils
import urllib
import jbm_backend
from . import models
import pandas as pd
from pathlib import Path

from sqlalchemy import text
from .sql_queries import bus_data_cte

app = jbm_backend.app
db = jbm_backend.db


file_path = Path(__file__).parent / "TCU500 Vehicle Mapping 19-8-23_2.xlsx"
df = pd.read_excel(file_path)

CORS(
    app,
    resources={r"/api/*": {"origins": "*", "expose_headers": "Authorization"}},
)


with open("./specificBusData.json") as f:
    specific_bus_data = json.load(f)

with open("./faultData.json") as f:
    faults_data = json.load(f)


with open("./busData.json") as f:
    buses_data = json.load(f)


with open("./specificFaultData.json") as f:
    specific_fault_data = json.load(f)


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET"
    return response


bus_statuses = [
    "in-depot",
    "in-field",
    "charging",
    "discharging",
    "disconnected",
    "full-charged",
    "in-fault",
    "idle",
    "all",
]


@app.route("/api/v1/app/<appName>/bus/total", methods=["GET"])
def get_total_buses(appName):
    # Filter the data based on the bus status
    # positions = models.TraccerDevices.query.limit(5).all()
    # CAN = models.CANFrame.query.limit(5).all()
    def get_data(_type):
        if _type == "all":
            query = f"""
                {bus_data_cte}
                 SELECT COUNT(*) FROM BUS_BATTERY_DATA  WHERE name != CAST(imei AS TEXT);  
                """

        else:
            query = f"""
                {bus_data_cte}
                 SELECT COUNT(*) FROM BUS_BATTERY_DATA WHERE '{_type}'= ANY(status)  AND name != CAST(imei AS TEXT);  
                """
        return list(db.session.execute(text(query)))[0][0]

    result_dict = {i: get_data(i) for i in bus_statuses}
    # result_dict["total"] = result_dict["all"]
    return jsonify({"status": "success", "data": result_dict})


def round_wrapper(x, to):
    try:
        return round(x, to)
    except Exception:
        return x


def does_bus_data_satisfy_filters(data, filters):
    is_true = True
    for k, _v in filters.items():
        try:
            v = int(_v)
        except Exception:
            v = _v
        match k:
            case "busNumber" if v.strip() != "":
                is_true &= "busNumber" in data and v == data[k]
            case "batteryNumber" if v.strip() != "-":
                is_true &= "battery" in data and v == data["battery"]
            case "deviceStatusType" if v.strip() != "-":
                is_true = "status" in data and v == data["status"]
            case "cityWise" if v.strip() != "-":
                # Apply filtering logic based on cityWise
                is_true &= (
                    "location" in data
                    and "address" in data["location"]
                    and utils.get_district_name(data["location"]["address"])
                    == v
                )
            case "depotWise" if v.strip() != "-":
                # Apply filtering logic based on depotWise
                is_true &= "depotNumber" in data and v == data["depotNumber"]
            case "soCRange":
                # Apply filtering logic based on soCRange
                is_true &= (
                    "batteryOverview" in data
                    and "soc" in data["batteryOverview"]
                    and v[0] <= data["batteryOverview"]["soc"]["value"] <= v[1]
                )
            case "soHRange":
                # Apply filtering logic based on soHRange
                is_true &= (
                    "batteryOverview" in data
                    and "soh" in data["batteryOverview"]
                    and v[0] <= data["batteryOverview"]["soh"]["value"] <= v[1]
                )
            case "cycleCount":
                # Apply filtering logic based on cycleCount
                is_true &= True
            case "voltage":
                # Apply filtering logic based on voltage
                is_true &= (
                    "batteryOverview" in data
                    and "voltage" in data["batteryOverview"]
                    and v[0]
                    <= data["batteryOverview"]["voltage"]["value"]
                    <= v[1]
                )
            case "temperatureRange":
                is_true &= (
                    "batteryOverview" in data
                    and "temperature" in data["batteryOverview"]
                    and v[0]
                    <= data["batteryOverview"]["temperature"]["value"]
                    <= v[1]
                )

            case "cellDiffInBatteryPackRange":
                # Apply filtering logic based on cellDiffInBatteryPackRange
                is_true &= True
            case "voltageDiffInBatteryPackRange":
                is_true &= (
                    "batteryOverview" in data
                    and "cellVoltageDelta" in data["batteryOverview"]
                    and v[0]
                    <= data["batteryOverview"]["cellVoltageDelta"]["min"]
                    <= data["batteryOverview"]["cellVoltageDelta"]["max"]
                    <= v[1]
                )
                # Apply filtering logic based on voltageDiffInBatteryPackRange
            case "deviceDiffDisconnectConditions":
                # Apply filtering logic based on deviceDiffDisconnectConditions
                is_true &= True
            case "faultLevelWise":
                # Apply filtering logic based on faultLevelWise
                is_true &= True

    return is_true


def get_results_dict(db, query):
    results = db.session.execute(text(query))
    results_keys = list(results.keys())
    results_dict = list(
        map(lambda x: dict(zip(results_keys, x)), list(results))
    )
    return results_dict


@app.route("/api/v1/app/<appName>/bus/<busStatus>", methods=["GET"])
def get_buses_data(appName, busStatus):
    mapping = {
        "in-depot": "in-depot",
        "in-field": "in-field",
        "charging": "charging",
        "discharging": "discharging",
        "disconnected": "disconnected",
        "full-charged": "full-charged",
        "in-fault": "in-fault",
        "idle": "idle",
    }
    reverse_mapping = {v: k for k, v in mapping.items()}
    filters = request.args.get("filters", None)
    try:
        decoded_filters = urllib.parse.unquote(filters)
        decoded_filters = json.loads(decoded_filters)

    except Exception:
        decoded_filters = None

    if busStatus not in bus_statuses:
        return (
            jsonify({"status": "error", "message": "Invalid bus status."}),
            400,
        )

    limit = int(request.args.get("limit", -1))
    offset = int(request.args.get("offset", -1))
    limit = limit if limit > 0 else None
    offset = offset if offset >= 0 else None
    if busStatus != "all":
        filtered_data = [
            bus for bus in buses_data if bus["status"] == busStatus
        ]
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM BUS_BATTERY_DATA  WHERE '{mapping[busStatus]}' = ANY(status) """
        if limit and offset:
            query += f"LIMIT {limit} OFFSET {offset}"

    else:
        if decoded_filters is not None:
            filtered_data = [
                bus
                for bus in buses_data
                if does_bus_data_satisfy_filters(bus, decoded_filters)
            ]
            bus_data_cte
        else:
            filtered_data = buses_data

        query = f"""
                {bus_data_cte}
                 SELECT  *  FROM BUS_BATTERY_DATA  """
        if limit and offset:
            query += f"LIMIT {limit} OFFSET {offset}"

    results_list = get_results_dict(db, query)

    x = filtered_data[0]
    filtered_data = []
    if len(results_list) > 0:
        for k, i in enumerate(results_list):
            busN = "".join(i["name"].split(" ")) if i["name"] else ""
            if (
                not i["latitude"]
                or not i["longitude"]
                or str(busN) == str(i["imei"])
            ):
                continue
            t = {
                **x,
                **{
                    "uuid": i["imei"],
                    "busNumber": busN,
                    "IMEI": i["imei"],
                    "status": (
                        (
                            next(
                                (
                                    reverse_mapping[item]
                                    for item in i["status"]
                                    if item in reverse_mapping
                                ),
                                "",
                            )
                        )
                        if busStatus == "all"
                        else busStatus
                    ),
                    "depotNumber": i["depot"],
                    "battery": i["city"],
                    "location": {
                        "address": "Narnaul, Mahendragarh District, Haryana, 123001, India",
                        "coordinates": {
                            "lat": i["latitude"],
                            "lng": i["longitude"],
                        },
                    },
                    "statusOptions": {
                        "bus": {"text": "Online", "status": "on"},
                        "CANData": {"text": "CAN Data", "status": "on"},
                        "externalPower": {
                            "text": "External Power",
                            "status": "on",
                        },
                        "deviceData": {"text": "Device Data", "status": "on"},
                        "GPSData": {"text": "GPS Data", "status": "on"},
                        "busRunning": {"text": "Bus Running", "status": "on"},
                    },
                    "batteryOverview": {
                        "soc": {
                            "text": "SoC",
                            "value": round_wrapper(i["soc"], 2),
                            "units": "%",
                        },
                        "soh": {
                            "text": "SoH",
                            "value": round_wrapper(i["soh"], 2),
                            "units": "%",
                        },
                        "temperature": {
                            "text": "Temperature",
                            "value": 76.1,
                            "units": "\u00b0C",
                        },
                        "voltage": {
                            "text": "Voltage",
                            "value": round_wrapper(i["voltage"], 2),
                            "units": "V",
                        },
                        "current": {
                            "text": "Current",
                            "value": round_wrapper(i["total_current"], 2),
                            "units": "A",
                        },
                        "regenation": {
                            "text": "Regeneration",
                            "value": "Disabled",
                        },
                        "BMSStatus": {"text": "BMS Status", "value": "Normal"},
                        "speed": {
                            "text": "Speed",
                            "value": 81,
                            "units": "km/h",
                        },
                        "contractorStatus": {
                            "text": "String Contractor Status",
                            "value": "Closed",
                        },
                        "cellVoltageDelta": {
                            "text": "String-Wise Delta of Cell Voltage",
                            "min": 31.48,
                            "max": 81.28,
                            "units": "mV",
                        },
                        "temperatureDelta": {
                            "text": "String-Wise Delta of Temperature",
                            "min": 27.81,
                            "max": 78.74,
                            "units": "\u00b0C",
                        },
                    },
                },
            }

            filtered_data.append(t)

    paginated_data = filtered_data
    next_offset = None
    has_more = None
    if offset and limit:
        next_offset = offset + limit
        if busStatus != "all":
            q = f"""
        {bus_data_cte}
        SELECT COUNT(*) FROM BUS_BATTERY_DATA  WHERE '{mapping[busStatus]}' = ANY(status) AND name != CAST(imei AS TEXT)                 
        """
        else:
            q = f"""
        {bus_data_cte}
        SELECT COUNT(*) FROM BUS_BATTERY_DATA  WHERE name != CAST(imei AS TEXT)             
        """
        has_more = next_offset < list(db.session.execute(text(q)))[0][0]

    next_url = (
        (
            f"/api/v1/app/{appName}/bus/{busStatus}?"
            + (
                "limit={limit}&offset={next_offset}"
                if limit and offset and next_offset
                else ""
            )
        )
        if has_more
        else None
    )
    return jsonify(
        {
            "status": "success",
            "data": {
                "buses": paginated_data,
                "length": len(paginated_data),
            },
            "next": next_url,
        }
    )


@app.route("/api/v1/app/<appName>/bus/all/<uuid>", methods=["GET"])
def get_bus_by_uuid(appName, uuid):
    if uuid == 0 or uuid == "0":
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM BUS_BATTERY_DATA  LIMIT 1
        """
    else:
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM BUS_BATTERY_DATA  WHERE imei = {uuid}
        """

    results_dict = get_results_dict(db, query)
    x = specific_bus_data[0]
    filtered_data = []
    if results_dict:
        for k, i in enumerate(results_dict):
            busN = "".join(i["name"].split(" ")) if i["name"] else ""
            if (
                not i["latitude"]
                or not i["longitude"]
                or str(busN) == str(i["imei"])
            ):
                continue
            t = {
                **x,
                **{
                    "uuid": i["imei"],
                    "busNumber": busN,
                    "IMEI": i["imei"],
                    "status": ", ".join(i["status"]),
                    "depotNumber": i["depot"],
                    "battery": i["city"],
                    "location": {
                        "address": "Narnaul, Mahendragarh District, Haryana, 123001, India",
                        "coordinates": {
                            "lat": i["latitude"],
                            "lng": i["longitude"],
                        },
                    },
                    "batteryOverview": {
                        "soc": {
                            "text": "SoC",
                            "value": round_wrapper(i["soc"], 2),
                            "units": "%",
                        },
                        "soh": {
                            "text": "SoH",
                            "value": round_wrapper(i["soh"], 2),
                            "units": "%",
                        },
                        "temperature": {
                            "text": "Temperature",
                            "value": 76.1,
                            "units": "\u00b0C",
                        },
                        "voltage": {
                            "text": "Voltage",
                            "value": round_wrapper(i["voltage"], 2),
                            "units": "V",
                        },
                        "current": {
                            "text": "Current",
                            "value": round_wrapper(i["total_current"], 2),
                            "units": "A",
                        },
                        "regenation": {
                            "text": "Regeneration",
                            "value": "Disabled",
                        },
                        "BMSStatus": {"text": "BMS Status", "value": "Normal"},
                        "speed": {
                            "text": "Speed",
                            "value": 81,
                            "units": "km/h",
                        },
                        "contractorStatus": {
                            "text": "String Contractor Status",
                            "value": "Closed",
                        },
                        "cellVoltageDelta": {
                            "text": "String-Wise Delta of Cell Voltage",
                            "min": 31.48,
                            "max": 81.28,
                            "units": "mV",
                        },
                        "temperatureDelta": {
                            "text": "String-Wise Delta of Temperature",
                            "min": 27.81,
                            "max": 78.74,
                            "units": "\u00b0C",
                        },
                    },
                },
            }

            filtered_data.append(t)
    bus_data = next(iter(filtered_data), None)
    if not bus_data:
        return jsonify({"status": "error", "message": "Bus not found"}), 404

    return jsonify({"status": "success", "data": bus_data})


@app.route("/api/v1/app/<appName>/fault/<faultStatus>", methods=["GET"])
def get_faults_data(appName, faultStatus):
    filters = request.args.get("filters", None)

    try:
        decoded_filters = urllib.parse.unquote(filters)
        decoded_filters = json.loads(decoded_filters)

    except Exception:
        decoded_filters = None

    if faultStatus not in bus_statuses:
        return (
            jsonify({"status": "error", "message": "Invalid bus status."}),
            400,
        )

    # Filter the data based on the bus status
    if faultStatus != "all":
        filtered_data = [
            fault for fault in faults_data if fault["status"] == faultStatus
        ]
    else:
        if decoded_filters is not None:
            bus_numbers = [
                bus["busNumber"]
                for bus in buses_data
                if does_bus_data_satisfy_filters(bus, decoded_filters)
            ]

            filtered_data = [
                fault
                for fault in faults_data
                if fault["busNumber"] in bus_numbers
            ]
        else:
            filtered_data = faults_data

    limit = int(request.args.get("limit", 10))
    offset = int(request.args.get("offset", 0))

    paginated_data = filtered_data[offset : offset + limit]

    next_offset = offset + limit
    has_more = next_offset < len(faults_data)

    next_url = (
        f"/api/v1/app/{appName}/fault/{faultStatus}?limit={limit}&offset={next_offset}"
        if has_more
        else None
    )
    return jsonify(
        {
            "status": "success",
            "data": {"faults": paginated_data, "length": len(faults_data)},
            "next": next_url,
        }
    )


@app.route("/api/v1/app/<appName>/fault/all/<uuid>", methods=["GET"])
def get_fault_by_uuid(appName, uuid):
    fault_data = list(
        (fault for fault in specific_fault_data if fault["uuid"] == uuid)
    )

    if not fault_data:
        return jsonify({"status": "error", "message": "Faults not found"}), 404

    return jsonify({"status": "success", "data": {"faultedBuses": fault_data}})


def prepare_filters(fields):
    for k in fields.keys():
        match k:
            case "batteryNumber":
                # Apply filtering logic based on batteryNumber
                vals = set([i["battery"] for i in buses_data])
                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]
                fields[k]["initialValue"] = "-"
            case "deviceStatusType":
                # Apply filtering logic based on deviceStatusType
                vals = set([i["status"] for i in buses_data])
                fields[k]["options"] = [
                    {"label": utils.kebab_to_title(i), "value": i}
                    for i in vals
                ]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]

                fields[k]["initialValue"] = "-"
            case "cityWise":
                # Apply filtering logic based on cityWise
                vals = set(
                    [
                        utils.get_district_name(i["location"]["address"])
                        for i in buses_data
                    ]
                )

                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]
                fields[k]["initialValue"] = "-"

            case "depotWise":
                # Apply filtering logic based on depotWise
                vals = set([i["depotNumber"] for i in buses_data])
                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["options"].append({"label": "Any", "value": "-"})
                fields[k]["initialValue"] = "-"
            case "soCRange":
                ...

            case "soHRange":
                # Apply filtering logic based on soHRange
                ...

            case "cycleCount":
                # Apply filtering logic based on cycleCount
                ...
            case "voltage":
                # Apply filtering logic based on voltage
                ...

            case "temperatureRange":
                ...

            case "cellDiffInBatteryPackRange":
                # Apply filtering logic based on cellDiffInBatteryPackRange
                ...
            case "voltageDiffInBatteryPackRange":
                ...

                # Apply filtering logic based on voltageDiffInBatteryPackRange
            case "deviceDiffDisconnectConditions":
                # Apply filtering logic based on deviceDiffDisconnectConditions
                ...
            case "faultLevelWise":
                # Apply filtering logic based on faultLevelWise
                vals = set([i["faultCode"] for i in faults_data])
                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]
                fields[k]["initialValue"] = "-"

            case _:
                # Handle the case for an unknown filter key (optional)
                ...
    return fields


with open("./filterstate.json") as f:
    data_filters = [
        {"fieldName": k, "fieldSpec": v}
        for k, v in prepare_filters(json.load(f)).items()
    ]


@app.route("/api/v1/app/<appName>/bus/all/filters-spec", methods=["GET"])
def get_filter_specification(appName):
    try:
        return jsonify({"status": "success", "data": {"fields": data_filters}})
    except Exception:
        return (
            jsonify({"status": "error", "message": "Fields specs not found"}),
            404,
        )


if __name__ == "__main__":
    app.run(debug=True)
