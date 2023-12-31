from collections import Counter
import datetime
from decimal import Decimal
from itertools import chain
import json
from uuid import uuid4
from flask import jsonify, request
from flask_cors import CORS
import urllib
from jbm_backend import app, db
from decimal import Decimal
from sqlalchemy import text
from .utils import round_wrapper
from datetime import datetime

# from .sql_queries import bus_data_cte

bus_data_cte = ""

CORS(
    app,
    resources={r"/api/*": {"origins": "*", "expose_headers": "Authorization"}},
)

schedule_thread = None


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


def apply_filters_to_bus_data(filters):
    where_clauses = []
    for k, _v in filters.items():
        try:
            v = int(_v)
        except Exception:
            v = _v
        if isinstance(v, str):
            v = v.strip()
        match k:
            case "busNumber" if v.strip() != "":
                where_clauses.append(f"'{v}' ILIKE bus_number")
            case "city" if v.strip() != "-":
                # Apply filtering logic based on cityWise
                where_clauses.append(f"'{v}' ILIKE city")
            case "depot" if v.strip() != "-":
                # Apply filtering logic based on depotWise
                where_clauses.append(f"'{v}' ILIKE depot")
            case "soc":
                # Apply filtering logic based on soCRange
                where_clauses.append(
                    f" (soc IS NULL OR soc BETWEEN {v[0]} AND {v[1]})"
                )

            case "soh":
                # Apply filtering logic based on soHRange
                where_clauses.append(
                    f" (soh IS NULL OR soh BETWEEN {v[0]} AND {v[1]})"
                )

            case "inletTemperature":
                # Apply filtering logic based on soHRange
                where_clauses.append(
                    f" (inlet_temperature IS NULL OR soh BETWEEN {v[0]} AND {v[1]})"
                )

            case "outletTemperature":
                # Apply filtering logic based on soHRange
                where_clauses.append(
                    f" (outlet_temperature IS NULL OR soh BETWEEN {v[0]} AND {v[1]})"
                )

            # case "voltage":
            #     # Apply filtering logic based on voltage
            #     is_true &= (
            #         "batteryOverview" in data
            #         and "voltage" in data["batteryOverview"]
            #         and v[0]
            #         <= data["batteryOverview"]["voltage"]["value"]
            #         <= v[1]
            #     )
            # # case "temperatureRange":
            # #     is_true &= (
            # #         "batteryOverview" in data
            # #         and "temperature" in data["batteryOverview"]
            # #         and v[0]
            # #         <= data["batteryOverview"]["temperature"]["value"]
            # #         <= v[1]
            # #     )

            case "cellDiffInBatteryPackRange":
                where_clauses.append(
                    f"""(
                        (delta_cell_v1 >= {v[0]} AND delta_cell_v1 <= {v[1]}) OR
                        (delta_cell_v2 >= {v[0]} AND delta_cell_v2 <= {v[1]}) OR 
                        (delta_cell_v3 >= {v[0]} AND delta_cell_v3 <= {v[1]}) OR
                        (delta_cell_v4 >= {v[0]} AND delta_cell_v4 <= {v[1]})
                        )



                    """
                )

            # case "voltageDiffInBatteryPackRange":
            #     is_true &= (
            #         "batteryOverview" in data
            #         and "cellVoltageDelta" in data["batteryOverview"]
            #         and v[0]
            #         <= data["batteryOverview"]["cellVoltageDelta"]["min"]
            #         <= data["batteryOverview"]["cellVoltageDelta"]["max"]
            #         <= v[1]
            #     )
            #     # Apply filtering logic based on voltageDiffInBatteryPackRange
            # case "deviceDiffDisconnectConditions":
            #     # Apply filtering logic based on deviceDiffDisconnectConditions
            #     is_true &= True
            # case "faultLevelWise":
            #     # Apply filtering logic based on faultLevelWise
            #     is_true &= True
            case "temperatureInlet":
                # Apply filtering logic based on soHRange
                where_clauses.append(
                    f" (inlet_temperature IS NULL OR inlet_temperature BETWEEN {v[0]} AND {v[1]})"
                )

            case "temperatureOutlet":
                # Apply filtering logic based on soHRange
                where_clauses.append(
                    f" (outlet_temperature IS NULL OR outlet_temperature BETWEEN {v[0]} AND {v[1]})"
                )

            case "temperature":
                where_clauses.append(
                    f"""(
                        ({v[0]} <= min_cell_t1 AND max_cell_t1 <= {v[1]}) OR
                        ({v[0]} <= min_cell_t2 AND max_cell_t2 <= {v[1]}) OR
                        ({v[0]} <= min_cell_t3 AND max_cell_t3 <= {v[1]}) OR
                        ({v[0]} <= min_cell_t4 AND max_cell_t4 <= {v[1]})
                        )



                    """
                )

    where_clause = " AND ".join(where_clauses)
    return where_clause


@app.route("/api/v1/app/<appName>/bus/total", methods=["GET"])
def get_total_buses(appName):
    def get_data(_type):
        if _type == "all":
            query = f"""
                {bus_data_cte}
                 SELECT COUNT(*) FROM bus_battery_data        """
            if decoded_filters is not None:
                query += (
                    f" WHERE {apply_filters_to_bus_data(decoded_filters)} "
                )
            else:
                query = query

        else:
            query = f"""
                {bus_data_cte}
                 SELECT COUNT(*) FROM bus_battery_data WHERE '{_type}'= ANY(status)    """
            if decoded_filters is not None:
                query += f" AND {apply_filters_to_bus_data(decoded_filters)} "
            else:
                query = query

        return list(db.session.execute(text(query)))[0][0]

    filters = request.args.get("filters", None)

    try:
        decoded_filters = urllib.parse.unquote(filters)
        decoded_filters = json.loads(decoded_filters)

    except Exception:
        decoded_filters = None

    result_dict = {i: get_data(i) for i in bus_statuses}
    return jsonify({"status": "success", "data": result_dict})


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


@app.route("/api/v1/app/<appName>/bus", methods=["GET"])
def get_buses_data(appName):
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
    bus_status = request.args.get("status", None)
    try:
        decoded_filters = urllib.parse.unquote(filters)
        decoded_filters = json.loads(decoded_filters)

    except Exception:
        decoded_filters = None

    if bus_status not in bus_statuses:
        return (
            jsonify({"status": "error", "message": "Invalid bus status."}),
            400,
        )

    limit = int(request.args.get("limit", -1))
    offset = int(request.args.get("offset", -1))
    limit = limit if limit > 0 else None
    offset = offset if offset >= 0 else None
    if bus_status != "all":
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM bus_battery_data  WHERE '{mapping[bus_status]}' = ANY(status) """
        if limit and offset:
            query += f"LIMIT {limit} OFFSET {offset}"

    else:
        query = f"""
                {bus_data_cte}
                 SELECT  *  FROM bus_battery_data  """
        if decoded_filters is not None:
            query += "  WHERE  " + apply_filters_to_bus_data(decoded_filters)
        else:
            query = query

        if limit and offset:
            query += f"LIMIT {limit} OFFSET {offset}"

    results_list = get_results_dict(db, query)

    filtered_data = []
    if len(results_list) > 0:
        for _, i in enumerate(results_list):
            t = {
                "uuid": i.get("imei", "") or "0",
                "busNumber": i.get("bus_number", "") or "",
                "IMEI": i.get("imei", "") or "",
                "timestamp": i.get(
                    "timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
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
                    if bus_status == "all"
                    else bus_status
                ),
                "depot": (i.get("depot", "") or "").title(),
                "city": (i.get("city", "") or "").title(),
                "location": {
                    "address": f"Somewhere in {(i['depot'] or '').title()}",
                    "coordinates": {
                        "lat": i["latitude"],
                        "lng": i["longitude"],
                    },
                },
                "busType": i["bus_type"],

                "totalAlerts": i["total_alerts"],
                "statusOptions": {
                    "bus": {
                        "text": (
                            "Online" if i["bus_status"] == "on" else "Offline"
                        ),
                        "status": i["bus_status"],
                    },
                    "CANData": {
                        "text": "CAN Data",
                        "status": i.get("can_data_status", "off"),
                    },
                    "externalPower": {
                        "text": "External Power",
                        "status": i.get("external_power_status", "off"),
                    },
                    "deviceData": {"text": "Device Data", "status": "on"},
                    "GPSData": {"text": "GPS Data", "status": "on"},
                    "busRunning": {
                        "text": "Bus Running",
                        "status": i["bus_running_status"],
                    },
                },
                "batteryOverview": {
                    "__order__": [
                        "soc",
                        "soh",
                        "voltage",
                        "current",
                        "regeneration",
                        "BMSStatus",
                        "inletTemperature",
                        "outletTemperature",
                        "speed",
                        # "contractorStatus",
                        "cellVoltage1",
                        "cellVoltage2",
                        "cellVoltage3",
                        "cellVoltage4",
                        "cellTemperature1",
                        "cellTemperature2",
                        "cellTemperature3",
                        "cellTemperature4",
                    ],
                    "soc": {
                        "text": "SoC",
                        "value": round_wrapper(i["soc"], 3),
                        "units": "%",
                    },
                    "soh": {
                        "text": "SoH",
                        "value": round_wrapper(i["soh"], 3),
                        "units": "%",
                    },
                    "inletTemperature": {
                        "text": "Inlet Temperature",
                        "value": round_wrapper(i["inlet_temperature"], 3),
                        "units": "\u00b0C",
                    },
                    "outletTemperature": {
                        "text": "Outlet Temperature",
                        "value": round_wrapper(i["outlet_temperature"], 3),
                        "units": "\u00b0C",
                    },
                    "current": {
                        "text": "Current",
                        "value": round_wrapper(i["current"], 3),
                        "units": "A",
                    },
                    "voltage": {
                        "text": "Voltage",
                        "value": round_wrapper(i["voltage"], 3),
                        "units": "V",
                    },
                    "regeneration": {
                        "text": "Regeneration",
                        "value": (
                            "Enabled"
                            if i.get("regeneration_status", "") == "on"
                            else "Disabled"
                        ),
                    },
                    "BMSStatus": {
                        "text": "BMS Status",
                        "value": (i["bms_status"] or "").title(),
                    },
                    "speed": {
                        "text": "Speed",
                        "value": round_wrapper(i["speed"], 0),
                        "units": "km/h",
                    },
                    "contractorStatus": {
                        "text": "String Contractor Status",
                        "value": "Closed",
                    },
                    "cellVoltage1": {
                        "text": "Cell Voltage S1",
                        "min": round_wrapper(i["min_cell_v1"], 3),
                        "max": round_wrapper(i["max_cell_v1"], 3),
                        "delta": (round_wrapper(i["delta_cell_v1"], 3)),
                        "units": "V",
                    },
                    "cellVoltage2": {
                        "text": "Cell Voltage S2",
                        "min": round_wrapper(i["min_cell_v2"], 3),
                        "max": round_wrapper(i["max_cell_v2"], 3),
                        "delta": (round_wrapper(i["delta_cell_v2"], 3)),
                        "units": "V",
                    },
                    "cellVoltage3": {
                        "text": "Cell Voltage S3",
                        "min": round_wrapper(i["min_cell_v3"], 3),
                        "max": round_wrapper(i["max_cell_v3"], 3),
                        "delta": (round_wrapper(i["delta_cell_v3"], 3)),
                        "units": "V",
                    },
                    "cellVoltage4": {
                        "text": "Cell Voltage S4",
                        "min": round_wrapper(i["min_cell_v4"], 3),
                        "max": round_wrapper(i["max_cell_v4"], 3),
                        "delta": (round_wrapper(i["delta_cell_v4"], 3)),
                        "units": "V",
                    },
                    "cellTemperature1": {
                        "text": "Cell Temperature S1",
                        "min": round_wrapper(i["min_cell_t1"], 3),
                        "max": round_wrapper(i["max_cell_t1"], 2),
                        "units": "\u00b0C",
                    },
                    "cellTemperature2": {
                        "text": "Cell Temperature S2",
                        "min": round_wrapper(i["min_cell_t2"], 3),
                        "max": round_wrapper(i["max_cell_t2"], 3),
                        "units": "\u00b0C",
                    },
                    "cellTemperature3": {
                        "text": "Cell Temperature S3",
                        "min": round_wrapper(i["min_cell_t3"], 3),
                        "max": round_wrapper(i["max_cell_t3"], 3),
                        "units": "\u00b0C",
                    },
                    "cellTemperature4": {
                        "text": "Cell Temperature S4",
                        "min": round_wrapper(i["min_cell_t4"], 3),
                        "max": round_wrapper(i["max_cell_t4"], 3),
                        "units": "\u00b0C",
                    },
                },
            }

            filtered_data.append(t)

    paginated_data = filtered_data
    next_offset = None
    has_more = None
    if offset and limit:
        next_offset = offset + limit
        if bus_status != "all":
            q = f"""
        {bus_data_cte}
        SELECT COUNT(*) FROM bus_battery_data  WHERE '{mapping[bus_status]}' = ANY(status) AND bus_number != imei                 
        """
        else:
            q = f"""
        {bus_data_cte}
        SELECT COUNT(*) FROM bus_battery_data  WHERE bus_number != imei
        """
        has_more = next_offset < list(db.session.execute(text(q)))[0][0]

    next_url = (
        (
            f"/api/v1/app/{appName}/bus/{bus_status}?"
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


@app.route("/api/v1/app/<appName>/bus/<uuid>", methods=["GET"])
def get_bus_by_uuid(appName, uuid):
    if uuid == 0 or uuid == "0":
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM bus_battery_data  ORDER BY imei LIMIT 1
        """
    else:
        uuid = str(uuid)
        query = f"""
                {bus_data_cte}
                 SELECT  * FROM bus_battery_data  WHERE imei = '{uuid}'
        """

    results_dict = get_results_dict(db, query)
    filtered_data = []
    if results_dict:
        for _, i in enumerate(results_dict):
            t = {
                "uuid": i.get("imei", ""),
                "busNumber": i.get("bus_number", ""),
                "IMEI": i.get("imei", ""),
                "timestamp": i.get(
                    "timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
                "status": ", ".join(i.get("status", [""])),
                "depot": (i.get("depot", "") or "").title(),
                "city": (i.get("city", "") or "").title(),
                "location": {
                    "address": f"Somewhere in {(i['depot'] or '').title()}",
                    "coordinates": {
                        "lat": i["latitude"],
                        "lng": i["longitude"],
                    },
                },
                "signalStrength": i["signal_strength"] or 1,
                "busType": i["bus_type"],

                "totalAlerts": i["total_alerts"],
                "statusOptions": {
                    "bus": {
                        "text": (
                            "Online" if i["bus_status"] == "on" else "Offline"
                        ),
                        "status": i["bus_status"],
                    },
                    "CANData": {
                        "text": "CAN Data",
                        "status": i.get("can_data_status", "off"),
                    },
                    "externalPower": {
                        "text": "External Power",
                        "status": i.get("external_power_status", "off"),
                    },
                    "deviceData": {"text": "Device Data", "status": "on"},
                    "GPSData": {"text": "GPS Data", "status": "on"},
                    "busRunning": {
                        "text": "Bus Running",
                        "status": i["bus_running_status"],
                    },
                },
                "batteryOverview": {
                    "__order__": [
                        "soc",
                        "soh",
                        "voltage",
                        "current",
                        "regeneration",
                        "BMSStatus",
                        "inletTemperature",
                        "outletTemperature",
                        "speed",
                        # "contractorStatus",
                        "cellVoltage1",
                        "cellVoltage2",
                        "cellVoltage3",
                        "cellVoltage4",
                        "cellTemperature1",
                        "cellTemperature2",
                        "cellTemperature3",
                        "cellTemperature4",
                    ],
                    "soc": {
                        "text": "SoC",
                        "value": round_wrapper(i["soc"], 3),
                        "units": "%",
                    },
                    "soh": {
                        "text": "SoH",
                        "value": round_wrapper(i["soh"], 3),
                        "units": "%",
                    },
                    "inletTemperature": {
                        "text": "Inlet Temperature",
                        "value": round_wrapper(i["inlet_temperature"], 3),
                        "units": "\u00b0C",
                    },
                    "outletTemperature": {
                        "text": "Outlet Temperature",
                        "value": round_wrapper(i["outlet_temperature"], 3),
                        "units": "\u00b0C",
                    },
                    "current": {
                        "text": "Current",
                        "value": round_wrapper(i["current"], 3),
                        "units": "A",
                    },
                    "voltage": {
                        "text": "Voltage",
                        "value": round_wrapper(i["voltage"], 3),
                        "units": "V",
                    },
                    "regeneration": {
                        "text": "Regeneration",
                        "value": (
                            "Enabled"
                            if i.get("regeneration_status", "") == "on"
                            else "Disabled"
                        ),
                    },
                    "BMSStatus": {
                        "text": "BMS Status",
                        "value": (i["bms_status"] or "").title(),
                    },
                    "speed": {
                        "text": "Speed",
                        "value": round_wrapper(i["speed"], 0),
                        "units": "km/h",
                    },
                    "contractorStatus": {
                        "text": "String Contractor Status",
                        "value": "Closed",
                    },
                    "cellVoltage1": {
                        "text": "Cell Voltage S1",
                        "min": round_wrapper(i["min_cell_v1"], 3),
                        "max": round_wrapper(i["max_cell_v1"], 3),
                        "delta": (round_wrapper(i["delta_cell_v1"], 3)),
                        "units": "V",
                    },
                    "cellVoltage2": {
                        "text": "Cell Voltage S2",
                        "min": round_wrapper(i["min_cell_v2"], 3),
                        "max": round_wrapper(i["max_cell_v2"], 3),
                        "delta": (round_wrapper(i["delta_cell_v2"], 3)),
                        "units": "V",
                    },
                    "cellVoltage3": {
                        "text": "Cell Voltage S3",
                        "min": round_wrapper(i["min_cell_v3"], 3),
                        "max": round_wrapper(i["max_cell_v3"], 3),
                        "delta": (round_wrapper(i["delta_cell_v3"], 3)),
                        "units": "V",
                    },
                    "cellVoltage4": {
                        "text": "Cell Voltage S4",
                        "min": round_wrapper(i["min_cell_v4"], 3),
                        "max": round_wrapper(i["max_cell_v4"], 3),
                        "delta": (round_wrapper(i["delta_cell_v4"], 3)),
                        "units": "V",
                    },
                    "cellTemperature1": {
                        "text": "Cell Temperature S1",
                        "min": round_wrapper(i["min_cell_t1"], 3),
                        "max": round_wrapper(i["max_cell_t1"], 2),
                        "units": "\u00b0C",
                    },
                    "cellTemperature2": {
                        "text": "Cell Temperature S2",
                        "min": round_wrapper(i["min_cell_t2"], 3),
                        "max": round_wrapper(i["max_cell_t2"], 3),
                        "units": "\u00b0C",
                    },
                    "cellTemperature3": {
                        "text": "Cell Temperature S3",
                        "min": round_wrapper(i["min_cell_t3"], 3),
                        "max": round_wrapper(i["max_cell_t3"], 3),
                        "units": "\u00b0C",
                    },
                    "cellTemperature4": {
                        "text": "Cell Temperature S4",
                        "min": round_wrapper(i["min_cell_t4"], 3),
                        "max": round_wrapper(i["max_cell_t4"], 3),
                        "units": "\u00b0C",
                    },
                },
            }

            filtered_data.append(t)
    bus_data = next(iter(filtered_data), None)
    if not bus_data:
        return jsonify({"status": "error", "message": "Bus not found"}), 404

    return jsonify({"status": "success", "data": bus_data})


@app.route("/api/v1/app/<appName>/fault", methods=["GET"])
def get_faults_data(appName):
    limit = int(request.args.get("limit", -1))
    offset = int(request.args.get("offset", -1))
    limit = limit if limit > 0 else None
    offset = offset if offset >= 0 else None

    query = """
            SELECT  * FROM bus_faults_data 
            """
    if limit and offset:
        query += f"LIMIT {limit} OFFSET {offset}"

    results_list = get_results_dict(db, query)

    status_choices = [
        {
            "value": "open",
            "label": "Open",
        },
        {
            "value": "in-process",
            "label": "In Process",
        },
        {
            "value": "close",
            "label": "Close",
        },
    ]
    filtered_data = [
        {
            "uuid": str(uuid4()),
            "imei": res["imei"],
            "busNumber": res["bus_number"],
            "faultCode": res["fault_code"],
            "faultDescription": res["fault_description"],
            "faultStatus": next(
                (
                    i
                    for i in status_choices
                    if res["fault_status"] == i["value"]
                ),
                {"open": "Open"},
            ),
            "startTime": res["start_time"],
            "endTime": res["end_time"],
            "faultLevel": res["fault_level"],
            "faultDuration": res["fault_duration"],
            "timeToResolve": res["time_to_resolve"],
            "faultType": "Automatic",
            "location": {
                "coordinates": {
                    "lat": res["latitude"],
                    "lng": res["longitude"],
                },
            },
            "ticketNumber": "T-01",
        }
        for res in results_list
    ]

    return jsonify(
        {
            "status": "success",
            "data": {"faults": filtered_data, "length": len(filtered_data)},
        }
    )


@app.route("/api/v1/app/<appName>/fault/<uuid>", methods=["GET"])
def get_fault_by_uuid(appName, uuid):
    limit = int(request.args.get("limit", -1))
    offset = int(request.args.get("offset", -1))
    limit = limit if limit > 0 else None
    offset = offset if offset >= 0 else None
    start_time = request.args.get("startTime", None)
    fault_code = request.args.get("faultCode", None)
    query = f"""
            SELECT  * FROM bus_faults_data WHERE imei ='{uuid}'
            """
    if start_time and fault_code:
        query += (
            f" AND start_time='{start_time}' AND fault_code='{fault_code}'"
        )
    if limit and offset:
        query += f"LIMIT {limit} OFFSET {offset}"

    results_list = get_results_dict(db, query)

    status_choices = [
        {
            "value": "open",
            "label": "Open",
        },
        {
            "value": "in-process",
            "label": "In Process",
        },
        {
            "value": "close",
            "label": "Close",
        },
    ]
    filtered_data = [
        {
            "uuid": str(uuid4()),
            "imei": res["imei"],
            "busNumber": res["bus_number"],
            "faultCode": res["fault_code"],
            "faultDescription": res["fault_description"],
            "faultStatus": next(
                (
                    i
                    for i in status_choices
                    if res["fault_status"] == i["value"]
                ),
                {"open": "Open"},
            ),
            "startTime": res["start_time"],
            "endTime": res["end_time"],
            "faultLevel": res["fault_level"],
            "faultDuration": res["fault_duration"],
            "faultType": "Automatic",
            "timeToResolve": res["time_to_resolve"],
            "location": {
                "coordinates": {
                    "lat": res["latitude"],
                    "lng": res["longitude"],
                },
            },
            "ticketNumber": "T-01",
        }
        for res in results_list
    ]
    return jsonify({"status": "success", "data": {"faults": filtered_data}})


def prepare_filters(fields):
    for k in fields.keys():
        match k:
            case "city":
                query = f"""
                {bus_data_cte}
                 SELECT DISTINCT city FROM bus_battery_data WHERE city IS NOT NULL AND city != ''
        """

                vals = get_results_list(db, query)
                fields[k]["initialValue"] = "-"

                fields[k]["options"] = [
                    {
                        "label": i.title(),
                        "value": i,
                    }
                    for i in vals
                ]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]

            case "depot":
                query = f"""
                {bus_data_cte}
                 SELECT DISTINCT depot FROM bus_battery_data WHERE depot IS NOT NULL AND depot != ''
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = "-"
                fields[k]["options"] = [
                    {
                        "label": i.title(),
                        "value": i,
                    }
                    for i in vals
                ]
                fields[k]["options"] = [
                    {"label": "Any", "value": "-"}
                ] + fields[k]["options"]

            case "soc":
                query = f"""
                {bus_data_cte}
                SELECT MIN(soc), MAX(soc) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100

            case "soh":
                query = f"""
                {bus_data_cte}
                SELECT MIN(soh), MAX(soh) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100

            case "voltage":
                query = f"""
                {bus_data_cte}
                SELECT MIN(voltage), MAX(voltage) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise

                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100

            case "cellDiffInBatteryPackRange":
                query = f"""
                    {bus_data_cte}
                 SELECT MIN(LEAST(delta_cell_v1, delta_cell_v2, delta_cell_v3, delta_cell_v4)) ,
                        MAX(GREATEST(delta_cell_v1, delta_cell_v2, delta_cell_v3, delta_cell_v4)) 
                 FROM bus_battery_data
                    """

                vals = get_results_list(db, query)
                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100
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

            case "temperatureInlet":
                query = f"""
                {bus_data_cte}
                SELECT MIN(inlet_temperature), MAX(inlet_temperature) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100

            case "temperatureOutlet":
                query = f"""
                {bus_data_cte}
                SELECT MIN(outlet_temperature), MAX(outlet_temperature) FROM bus_battery_data
        """

                vals = get_results_list(db, query)
                # Apply filtering logic based on depotWise
                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100

            case "temperature":
                query = f"""
                    {bus_data_cte}
                 SELECT MIN(LEAST(min_cell_t1, min_cell_t2, min_cell_t3, min_cell_t4)) ,
                        MAX(GREATEST(max_cell_t1, max_cell_t2, max_cell_t3, max_cell_t4)) 
                 FROM bus_battery_data
                    """

                vals = get_results_list(db, query)
                fields[k]["initialValue"] = vals
                fields[k]["min"] = vals[0]
                fields[k]["max"] = vals[1]
                fields[k]["step"] = (vals[1] - vals[0]) / 100

            case _:
                # Handle the case for an unknown filter key (optional)
                ...

    return fields


@app.route("/api/v1/app/<appName>/filters-spec", methods=["GET"])
def get_filter_specification(appName):
    try:
        with open("./filterstate.json") as f:
            data_filters = {
                k: v for k, v in prepare_filters(json.load(f)).items()
            }

        return jsonify(
            {
                "status": "success",
                "data": {
                    "fields": data_filters,
                },
                "__order__": [
                    "city",
                    "depot",
                    "soc",
                    "soh",
                    "cellDiffInBatteryPackRange",
                    "temperatureInlet",
                    "temperatureOutlet",
                    "temperature",
                ],
            }
        )
    except Exception as e:
        return (
            jsonify({"status": "error", "message": str(e)}),
            500,
        )


@app.route("/api/v1/app/<appName>/search", methods=["GET"])
def get_list(appName):
    try:
        query = "SELECT imei, bus_number FROM  bus_battery_data ORDER BY imei"
        vals = get_results_dict(db, query)
        d = list(
            map(
                lambda x: {
                    "label": " & ".join(map(str, x.values())),
                    "id": x["imei"],
                },
                vals,
            )
        )

        return jsonify(
            {
                "status": "success",
                "data": {
                    "search": d,
                },
            }
        )
    except Exception as e:
        return (
            jsonify({"status": "error", "message": str(e)}),
            500,
        )


@app.route("/refresh-view", methods=["GET"])
def refresh_view():
    error = ""
    try:
        db.session.execute(
            text(
                """
            REFRESH MATERIALIZED VIEW CONCURRENTLY bus_battery_data;
                """
            )
        )
        db.session.commit()
    except Exception as e:
        error += f"\n{e}"
    try:
        db.session.execute(
            text(
                """
                REFRESH MATERIALIZED VIEW CONCURRENTLY bus_faults_data;
                    """
            )
        )
        db.session.commit()
    except Exception as e:
        error += f"\n{e}"
    if error:
        return {"status": "error", "message": error}
    else:
        return {"status": "success", "message": "Refresh views"}


if __name__ == "__main__":
    app.run(debug=True)
