from collections import Counter
from decimal import Decimal
from itertools import chain
import json
from threading import Thread
import time
from flask import jsonify, request
from flask_cors import CORS
from jbm_backend import utils
import urllib
import jbm_backend
from decimal import Decimal, DecimalException

from sqlalchemy import text

# from .sql_queries import bus_data_cte

app = jbm_backend.app
db = jbm_backend.db

bus_data_cte = ""

CORS(
    app,
    resources={r"/api/*": {"origins": "*", "expose_headers": "Authorization"}},
)

schedule_thread = None

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


def refresh_materialized_view():
    print("Updated")
    db.session.execute(
        text(
            """
        REFRESH MATERIALIZED VIEW CONCURRENTLY bus_battery_data;
             """
        )
    )
    db.session.commit()


def run_schedule(app):
    with app.app_context():
        while True:
            refresh_materialized_view()
            time.sleep(30)


@app.route("/api/v1/app/<appName>/bus/total", methods=["GET"])
def get_total_buses(appName):
    # Filter the data based on the bus status
    # positions = models.TraccerDevices.query.limit(5).all()
    # CAN = models.CANFrame.query.limit(5).all()
    global schedule_thread
    if not schedule_thread:
        schedule_thread = Thread(target=lambda: run_schedule(app))
        schedule_thread.daemon = True
        schedule_thread.start()

    def get_data(_type):
        if _type == "all":
            query = f"""
                {bus_data_cte}
                 SELECT COUNT(*) FROM bus_battery_data  WHERE name != CAST(imei AS TEXT);  
                """

        else:
            query = f"""
                {bus_data_cte}
                 SELECT COUNT(*) FROM bus_battery_data WHERE '{_type}'= ANY(status)  AND name != CAST(imei AS TEXT);  
                """
        return list(db.session.execute(text(query)))[0][0]

    result_dict = {i: get_data(i) for i in bus_statuses}
    # result_dict["total"] = result_dict["all"]
    return jsonify({"status": "success", "data": result_dict})


def round_wrapper(x, to):
    try:
        if isinstance(x, str):
            x = Decimal(x)
        return round(float(x), to)
    except (ValueError, TypeError, DecimalException):
        return x


def apply_filters_to_bus_data(data, filters):
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
            # case "temperatureRange":
            #     is_true &= (
            #         "batteryOverview" in data
            #         and "temperature" in data["batteryOverview"]
            #         and v[0]
            #         <= data["batteryOverview"]["temperature"]["value"]
            #         <= v[1]
            #     )

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


def get_results_list(db, query):
    results = list(chain.from_iterable(db.session.execute(text(query))))
    result_list = [
        round_wrapper(float(value), 1) if isinstance(value, Decimal) else value
        for value in results
    ]
    return result_list


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
                 SELECT  * FROM bus_battery_data  WHERE '{mapping[busStatus]}' = ANY(status) """
        if limit and offset:
            query += f"LIMIT {limit} OFFSET {offset}"

    else:
        if decoded_filters is not None:
            filtered_data = [
                bus
                for bus in buses_data
                if apply_filters_to_bus_data(bus, decoded_filters)
            ]
            bus_data_cte
        else:
            filtered_data = buses_data

        query = f"""
                {bus_data_cte}
                 SELECT  *  FROM bus_battery_data  """
        if limit and offset:
            query += f"LIMIT {limit} OFFSET {offset}"

    results_list = get_results_dict(db, query)

    x = filtered_data[0]
    filtered_data = []
    if len(results_list) > 0:
        for _, i in enumerate(results_list):
            if not i["latitude"] or not i["longitude"]:
                continue
            t = {
                **x,
                **{
                    "uuid": i["imei"],
                    "busNumber": i["name"],
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
                    "totalAlerts": 0,
                    "statusOptions": {
                        "bus": {
                            "text": (
                                "Online"
                                if "Online" in i["status"]
                                else "Offline"
                            ),
                            "status": (
                                "off" if "Online" in i["status"] else "off"
                            ),
                        },
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
                        "__order__": [
                            "soc",
                            "soh",
                            "inletTemperature",
                            "outletTemperature",
                            "current",
                            "regeneration",
                            "speed",
                            "BMSStatus",
                            "contractorStatus",
                            "cellVoltageDelta1",
                            "cellVoltageDelta2",
                            "cellVoltageDelta3",
                            "cellVoltageDelta4",
                            "temperatureDelta1",
                            "temperatureDelta2",
                            "temperatureDelta3",
                            "temperatureDelta4",
                        ],
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
                        "inletTemperature": {
                            "text": "Inlet Temperature",
                            "value": round_wrapper(i["inlet_temperature"], 2),
                            "units": "\u00b0C",
                        },
                        "outletTemperature": {
                            "text": "Outlet Temperature",
                            "value": round_wrapper(i["outlet_temperature"], 2),
                            "units": "\u00b0C",
                        },
                        "current": {
                            "text": "Current",
                            "value": round_wrapper(i["total_current"], 2),
                            "units": "A",
                        },
                        "regeneration": {
                            "text": "Regeneration",
                            "value": "Disabled",
                        },
                        "BMSStatus": {"text": "BMS Status", "value": "Normal"},
                        "speed": {
                            "text": "Speed",
                            "value": round_wrapper(i["speed"], 2),
                            "units": "km/h",
                        },
                        "contractorStatus": {
                            "text": "String Contractor Status",
                            "value": "Closed",
                        },
                        "cellVoltageDelta1": {
                            "text": "String-Wise Delta of Cell Voltage 1",
                            "min": round_wrapper(i["min_cell_v1"], 2),
                            "max": round_wrapper(i["max_cell_v1"], 2),
                            "units": "mV",
                        },
                        "cellVoltageDelta2": {
                            "text": "String-Wise Delta of Cell Voltage 2",
                            "min": round_wrapper(i["min_cell_v2"], 2),
                            "max": round_wrapper(i["max_cell_v2"], 2),
                            "units": "mV",
                        },
                        "cellVoltageDelta3": {
                            "text": "String-Wise Delta of Cell Voltage 3",
                            "min": round_wrapper(i["min_cell_v3"], 2),
                            "max": round_wrapper(i["max_cell_v3"], 2),
                            "units": "mV",
                        },
                        "cellVoltageDelta4": {
                            "text": "String-Wise Delta of Cell Voltage 4",
                            "min": round_wrapper(i["min_cell_v4"], 2),
                            "max": round_wrapper(i["max_cell_v4"], 2),
                            "units": "mV",
                        },
                        "temperatureDelta1": {
                            "text": "String-Wise Delta of Temperature 1",
                            "min": round_wrapper(i["min_cell_t1"], 2),
                            "max": round_wrapper(i["max_cell_t1"], 2),
                            "units": "\u00b0C",
                        },
                        "temperatureDelta2": {
                            "text": "String-Wise Delta of Temperature 2",
                            "min": round_wrapper(i["min_cell_t2"], 2),
                            "max": round_wrapper(i["max_cell_t2"], 2),
                            "units": "\u00b0C",
                        },
                        "temperatureDelta3": {
                            "text": "String-Wise Delta of Temperature 3",
                            "min": round_wrapper(i["min_cell_t3"], 2),
                            "max": round_wrapper(i["max_cell_t3"], 2),
                            "units": "\u00b0C",
                        },
                        "temperatureDelta4": {
                            "text": "String-Wise Delta of Temperature 4",
                            "min": round_wrapper(i["min_cell_t4"], 2),
                            "max": round_wrapper(i["max_cell_t4"], 2),
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
        SELECT COUNT(*) FROM bus_battery_data  WHERE '{mapping[busStatus]}' = ANY(status) AND name != CAST(imei AS TEXT)                 
        """
        else:
            q = f"""
        {bus_data_cte}
        SELECT COUNT(*) FROM bus_battery_data  WHERE name != CAST(imei AS TEXT)             
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
                 SELECT  * FROM bus_battery_data  LIMIT 1
        """
    else:
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM bus_battery_data  WHERE imei = {uuid}
        """

    results_dict = get_results_dict(db, query)
    x = specific_bus_data[0]
    filtered_data = []
    if results_dict:
        for k, i in enumerate(results_dict):
            if not i["latitude"] or not i["longitude"]:
                continue
            t = {
                **x,
                **{
                    "uuid": i["imei"],
                    "busNumber": i["name"],
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
                    "totalAlerts": 0,
                    "statusOptions": {
                        "bus": {
                            "text": (
                                "Online"
                                if "Online" in i["status"]
                                else "Offline"
                            ),
                            "status": (
                                "off" if "Online" in i["status"] else "off"
                            ),
                        },
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
                        "__order__": [
                            "soc",
                            "soh",
                            "inletTemperature",
                            "outletTemperature",
                            "current",
                            "regeneration",
                            "speed",
                            "BMSStatus",
                            "contractorStatus",
                            "cellVoltageDelta1",
                            "cellVoltageDelta2",
                            "cellVoltageDelta3",
                            "cellVoltageDelta4",
                            "temperatureDelta1",
                            "temperatureDelta2",
                            "temperatureDelta3",
                            "temperatureDelta4",
                        ],
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
                        "inletTemperature": {
                            "text": "Inlet Temperature",
                            "value": round_wrapper(i["inlet_temperature"], 2),
                            "units": "\u00b0C",
                        },
                        "outletTemperature": {
                            "text": "Outlet Temperature",
                            "value": round_wrapper(i["outlet_temperature"], 2),
                            "units": "\u00b0C",
                        },
                        "current": {
                            "text": "Current",
                            "value": round_wrapper(i["total_current"], 2),
                            "units": "A",
                        },
                        "regeneration": {
                            "text": "Regeneration",
                            "value": "Disabled",
                        },
                        "BMSStatus": {"text": "BMS Status", "value": "Normal"},
                        "speed": {
                            "text": "Speed",
                            "value": round_wrapper(i["speed"], 2),
                            "units": "km/h",
                        },
                        "contractorStatus": {
                            "text": "String Contractor Status",
                            "value": "Closed",
                        },
                        "cellVoltageDelta1": {
                            "text": "String-Wise Delta of Cell Voltage 1",
                            "min": round_wrapper(i["min_cell_v1"], 2),
                            "max": round_wrapper(i["max_cell_v1"], 2),
                            "units": "mV",
                        },
                        "cellVoltageDelta2": {
                            "text": "String-Wise Delta of Cell Voltage 2",
                            "min": round_wrapper(i["min_cell_v2"], 2),
                            "max": round_wrapper(i["max_cell_v2"], 2),
                            "units": "mV",
                        },
                        "cellVoltageDelta3": {
                            "text": "String-Wise Delta of Cell Voltage 3",
                            "min": round_wrapper(i["min_cell_v3"], 2),
                            "max": round_wrapper(i["max_cell_v3"], 2),
                            "units": "mV",
                        },
                        "cellVoltageDelta4": {
                            "text": "String-Wise Delta of Cell Voltage 4",
                            "min": round_wrapper(i["min_cell_v4"], 2),
                            "max": round_wrapper(i["max_cell_v4"], 2),
                            "units": "mV",
                        },
                        "temperatureDelta1": {
                            "text": "String-Wise Delta of Temperature 1",
                            "min": round_wrapper(i["min_cell_t1"], 2),
                            "max": round_wrapper(i["max_cell_t1"], 2),
                            "units": "\u00b0C",
                        },
                        "temperatureDelta2": {
                            "text": "String-Wise Delta of Temperature 2",
                            "min": round_wrapper(i["min_cell_t2"], 2),
                            "max": round_wrapper(i["max_cell_t2"], 2),
                            "units": "\u00b0C",
                        },
                        "temperatureDelta3": {
                            "text": "String-Wise Delta of Temperature 3",
                            "min": round_wrapper(i["min_cell_t3"], 2),
                            "max": round_wrapper(i["max_cell_t3"], 2),
                            "units": "\u00b0C",
                        },
                        "temperatureDelta4": {
                            "text": "String-Wise Delta of Temperature 4",
                            "min": round_wrapper(i["min_cell_t4"], 2),
                            "max": round_wrapper(i["max_cell_t4"], 2),
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
                if apply_filters_to_bus_data(bus, decoded_filters)
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
            case "cityWise":
                # Apply filtering logic based on cityWise
                query = f"""
                {bus_data_cte}
                 SELECT DISTINCT UPPER(TRIM(city)) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                fields[k]["initialValue"] = "-"

                fields[k]["options"] = [
                    {
                        "label": i,
                        "value": utils.space_to_lowercase_kebab_case(i),
                    }
                    for i in vals
                ]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]

            case "depotWise":
                query = f"""
                {bus_data_cte}
                 SELECT DISTINCT UPPER(TRIM(depot)) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = "-"
                fields[k]["options"] = [
                    {
                        "label": i,
                        "value": utils.space_to_lowercase_kebab_case(i),
                    }
                    for i in vals
                ]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]

            case "soCRange":
                query = f"""
                {bus_data_cte}
                SELECT MIN(soc), MAX(soc) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = vals
                fields[k]["bounds"] = vals

            case "soHRange":
                query = f"""
                {bus_data_cte}
                SELECT MIN(soh), MAX(soh) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = vals
                fields[k]["bounds"] = vals

            case "voltage":
                query = f"""
                {bus_data_cte}
                SELECT MIN(voltage), MAX(voltage) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise

                fields[k]["initialValue"] = vals
                fields[k]["bounds"] = vals
            case "temperatureRange":
                ...
            #         query = f"""
            #         {bus_data_cte}
            #         SELECT MIN(temperature), MAX(temperature) FROM bus_battery_data
            # """

            #         vals = get_results_list(db, query)
            #         # Apply filtering logic based on depotWise
            #         fields[k]["initialValue"] = vals
            #         fields[k]["bounds"] = vals
            case "cellDiffInBatteryPackRange":
                ...
            #         # Apply filtering logic based on cellDiffInBatteryPackRange

            #         #         query = f"""
            #         #         {bus_data_cte}
            #         #         SELECT MIN(temperature), max(temperature) FROM bus_battery_data
            #         # """

            #         #         vals = get_results_list(db, query)
            #         #         # Apply filtering logic based on depotWise
            #         #         fields[k]["initialValue"] = vals
            #         ...

            case "voltageDiffInBatteryPackRange":
                ...
            #         query = f"""
            #         {bus_data_cte}
            #         SELECT MIN(min_cell_volt), MAX(max_cell_volt) FROM bus_battery_data
            # """

            #         vals = get_results_list(db, query)
            #         # Apply filtering logic based on depotWise
            #         fields[k]["initialValue"] = vals

            #         # Apply filtering logic based on voltageDiffInBatteryPackRange

            case _:
                # Handle the case for an unknown filter key (optional)
                ...
    return fields


@app.route("/api/v1/app/<appName>/bus/all/filters-spec", methods=["GET"])
def get_filter_specification(appName):
    try:
        with open("./filterstate.json") as f:
            data_filters = [
                {"fieldName": k, "fieldSpec": v}
                for k, v in prepare_filters(json.load(f)).items()
            ]
        return jsonify({"status": "success", "data": {"fields": data_filters}})
    except Exception as e:
        return (
            jsonify({"status": "error", "message": str(e)}),
            500,
        )


if __name__ == "__main__":
    app.run(debug=True)
