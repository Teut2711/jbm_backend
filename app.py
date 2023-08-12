from collections import Counter
import json
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from . import utils
import urllib
from dotenv import load_dotenv

load_dotenv()
db_config = {
    "username": os.getenv("BUCEPHALUS__TIMESCALEDB__USERNAME"),
    "password": os.getenv("BUCEPHALUS__TIMESCALEDB__PASSWORD"),
    "host": os.getenv("BUCEPHALUS__TIMESCALEDB__HOSTNAME"),
    "port": os.getenv("BUCEPHALUS__TIMESCALEDB__PORTNUMBER"),
    "database": os.getenv("BUCEPHALUS__TIMESCALEDB__DATABASE"),
}

DATABASE_URL = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL

db = SQLAlchemy(app)
with app.app_context():
    db.Model.metadata.reflect(db.engine)

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


@app.route("/api/v1/app/<appName>/bus/total", methods=["GET"])
def get_total_buses(appName):
    # Filter the data based on the bus status
    from jbm_backend import models

    positions = models.TraccerPositions.query.limit(5).all()

    # Print the positions
    for position in positions:
        print(f"Position ID: {position.id}")
        for field in position.__table__.columns.keys():
            print(f"{field}: {getattr(position, field)}")
        print("-" * 20)

    c = Counter([i["status"] for i in buses_data])
    c["total"] = len(buses_data)

    return jsonify({"status": "success", "data": c})


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


@app.route("/api/v1/app/<appName>/bus/<busStatus>", methods=["GET"])
def get_buses_data(appName, busStatus):
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

    # Filter the data based on the bus status
    from jbm_backend import models

    print(jsonify(models.TraccerPositions.query.limit(5).all()[0]))

    if busStatus != "all":
        filtered_data = [
            bus for bus in buses_data if bus["status"] == busStatus
        ]
    else:
        if decoded_filters is not None:
            filtered_data = [
                bus
                for bus in buses_data
                if does_bus_data_satisfy_filters(bus, decoded_filters)
            ]
        else:
            filtered_data = buses_data

    limit = int(request.args.get("limit", 10))
    offset = int(request.args.get("offset", 0))

    paginated_data = filtered_data[offset : offset + limit]

    next_offset = offset + limit
    has_more = next_offset < len(filtered_data)

    next_url = (
        f"/api/v1/app/{appName}/bus/{busStatus}?limit={limit}&offset={next_offset}"
        if has_more
        else None
    )
    return jsonify(
        {
            "status": "success",
            "data": {"buses": paginated_data, "length": len(filtered_data)},
            "next": next_url,
        }
    )


@app.route("/api/v1/app/<appName>/bus/all/<uuid>", methods=["GET"])
def get_bus_by_uuid(appName, uuid):
    if uuid == 0 or uuid == "0":
        bus_data = specific_bus_data[0]
    else:
        bus_data = next(
            filter(lambda x: x["uuid"] == uuid, specific_bus_data), None
        )

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
