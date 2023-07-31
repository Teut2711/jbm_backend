from collections import Counter
import json
from flask import Flask, jsonify, request
from flask_cors import CORS

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


def kebab_to_camel(kebab_str):
    words = kebab_str.split("-")
    return words[0] + "".join(word.capitalize() for word in words[1:])


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


@app.route("/api/v1/app/<appName>/bus/<busStatus>", methods=["GET"])
def get_buses_data(appName, busStatus):
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


@app.route("/api/v1/app/<appName>/fault", methods=["GET"])
def get_faults_data(appName):
    limit = int(request.args.get("limit", 10))
    offset = int(request.args.get("offset", 0))

    paginated_data = faults_data[offset : offset + limit]

    next_offset = offset + limit
    has_more = next_offset < len(faults_data)

    next_url = (
        f"/api/v1/app/{appName}/fault?limit={limit}&offset={next_offset}"
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


@app.route("/api/v1/app/<appName>/fault/<uuid>", methods=["GET"])
def get_fault_by_uuid(appName, uuid):
    fault_data = list(
        (fault for fault in specific_fault_data if fault["uuid"] == uuid)
    )

    if not fault_data:
        return jsonify({"status": "error", "message": "Bus not found"}), 404

    return jsonify({"status": "success", "data": {"faultedBuses": fault_data}})


if __name__ == "__main__":
    app.run(debug=True)
