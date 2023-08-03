from collections import Counter
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
from . import utils
import urllib

app = Flask(__name__)
CORS(
    app,
    resources={r"/api/*": {"origins": "*", "expose_headers": "Authorization"}},
)  # Add CORS support and expose "Authorization" header

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


def filter_list(data, filters):
    for k, _v in filters.items():
        try:
            v = int(_v)
        except Exception:
            v = _v
        match k:
            case "busNumber":
                return data.get("busNumber", None) and v == data[k]
            case "batteryNumber":
                # Apply filtering logic based on batteryNumber
                return data.get("battery", None) and v == data["battery"]
            case "deviceStatusType":
                # Apply filtering logic based on deviceStatusType
                return "status" in data and v == data["status"]
            case "cityWise":
                # Apply filtering logic based on cityWise
                return (
                    data.get("location", None)
                    and data["location"].get("address", None)
                    and utils.get_district_name(data["location"]["address"])
                    == v
                )
            case "depotWise":
                # Apply filtering logic based on depotWise
                return (
                    data.get("depotNumber", None) and v == data["depotNumber"]
                )
            case "soCRange":
                # Apply filtering logic based on soCRange
                return (
                    data.get("batteryOverview", None)
                    and data["batteryOverview"].get("soc", None)
                    and data["batteryOverview"]["soc"]["value"] == v
                )
            case "soHRange":
                # Apply filtering logic based on soHRange
                return (
                    data.get("batteryOverview", None)
                    and data["batteryOverview"].get("soh", None)
                    and data["batteryOverview"]["soh"]["value"] == v
                )
            case "cycleCount":
                # Apply filtering logic based on cycleCount
                return True
            case "voltage":
                # Apply filtering logic based on voltage
                return (
                    data.get("batteryOverview", None)
                    and data["batteryOverview"].get("voltage", None)
                    and data["batteryOverview"]["voltage"]["value"] == v
                )
            case "temperatureRange":
                return (
                    data.get("batteryOverview", None)
                    and data["batteryOverview"].get("temperature", None)
                    and data["batteryOverview"]["temperature"]["value"] == v
                )

            case "cellDiffInBatteryPackRange":
                # Apply filtering logic based on cellDiffInBatteryPackRange
                return True
            case "voltageDiffInBatteryPackRange":
                return (
                    data.get("batteryOverview", None)
                    and data["batteryOverview"].get("cellVoltageDelta", None)
                    and v[0]
                    <= data["batteryOverview"]["cellVoltageDelta"]["min"]
                    <= data["batteryOverview"]["cellVoltageDelta"]["max"]
                    <= v[1]
                )
                # Apply filtering logic based on voltageDiffInBatteryPackRange
            case "deviceDiffDisconnectConditions":
                # Apply filtering logic based on deviceDiffDisconnectConditions
                return True
            case "faultLevelWise":
                # Apply filtering logic based on faultLevelWise
                return True
            case _:
                # Handle the case for an unknown filter key (optional)
                return False


@app.route("/api/v1/app/<appName>/bus/<busStatus>", methods=["GET"])
def get_buses_data(appName, busStatus):
    filters = request.args.get("filters", None)
    try:
        decoded_filters = urllib.parse.unquote(filters)
        filters_data = json.loads(decoded_filters)

    except Exception:
        filters_data = None

    if busStatus not in bus_statuses:
        return (
            jsonify({"status": "error", "message": "Invalid bus status."}),
            400,
        )

    # Filter the data based on the bus status
    if busStatus != "all":
        filtered_data = [
            bus for bus in buses_data if bus["status"] == busStatus
        ]
        if filters_data:
            filtered_data = [
                bus for bus in buses_data if filter_list(bus, filters_data)
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


def transform_filter_fields(fields):
    for k in fields.keys():
        match k:
            case "batteryNumber":
                # Apply filtering logic based on batteryNumber
                vals = set([i["battery"] for i in buses_data])
                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["initialValue"] = fields[k]["options"][0]["value"]
                print(fields, vals)
            case "deviceStatusType":
                # Apply filtering logic based on deviceStatusType
                vals = set([i["status"] for i in buses_data])
                fields[k]["options"] = [
                    {"label": utils.kebab_to_title(i), "value": i}
                    for i in vals
                ]
                fields[k]["initialValue"] = fields[k]["options"][0]["value"]

            case "cityWise":
                # Apply filtering logic based on cityWise
                vals = set(
                    [
                        utils.get_district_name(i["location"]["address"])
                        for i in buses_data
                    ]
                )
                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["initialValue"] = fields[k]["options"][0]["value"]

            case "depotWise":
                # Apply filtering logic based on depotWise
                vals = set([i["depotNumber"] for i in buses_data])
                fields[k]["options"] = [{"label": i, "value": i} for i in vals]
                fields[k]["initialValue"] = fields[k]["options"][0]["value"]

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
                ...
            case _:
                # Handle the case for an unknown filter key (optional)
                ...
    return fields


with open("./filterstate.json") as f:
    data_filters = [
        {"fieldName": k, "fieldSpec": v}
        for k, v in transform_filter_fields(json.load(f)).items()
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
