import json
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(
    app,
    resources={r"/api/*": {"origins": "*", "expose_headers": "Authorization"}},
)  # Add CORS support and expose "Authorization" header

with open("./specificBusData.json") as f:
    specific_buses_data = json.load(f)

for i in specific_buses_data:
    print(i["uuid"])

with open("./busData.json") as f:
    buses_data = json.load(f)


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET"
    return response


@app.route("/api/v1/app/<appName>/bus/total", methods=["GET"])
def get_total_buses(appName):
    total_buses = len(buses_data)
    return jsonify({"status": "success", "data": {"total": total_buses}})


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
        bus_data = specific_buses_data[0]
    else:
        bus_data = next(
            filter(lambda x: x["uuid"] == uuid, specific_buses_data), None
        )

    if not bus_data:
        return jsonify({"status": "error", "message": "Bus not found"}), 404

    return jsonify({"status": "success", "data": bus_data})


@app.route("/api/v1/app/<appName>/fault", methods=["GET"])
def get_faults_data(appName, busStatus):
    # Your code to retrieve data from the database or dummy data
    # Replace this with actual database queries
    # Example: buses_data = YourModel.query.filter_by(status=busStatus).all()

    # For demonstration, we'll use the dummy data
    buses_data = dummy_buses_data

    # Implement pagination here if needed based on query parameters 'limit' and 'offset'
    limit = int(request.args.get("limit", 10))
    offset = int(request.args.get("offset", 0))
    paginated_data = buses_data[offset : offset + limit]

    return jsonify(
        {
            "status": "success",
            "data": {"buses": paginated_data, "length": len(buses_data)},
        }
    )


@app.route("/api/v1/app/<appName>/fault/<uuid>", methods=["GET"])
def get_fault_by_uuid(appName, uuid):
    # Your code to retrieve data from the database or dummy data
    # Replace this with actual database queries
    # Example: bus_data = YourModel.query.filter_by(uuid=uuid).first()

    # For demonstration, we'll use the dummy data
    bus_data = next(
        (bus for bus in dummy_buses_data if bus["uuid"] == uuid), None
    )

    if not bus_data:
        return jsonify({"status": "error", "message": "Bus not found"}), 404

    return jsonify({"status": "success", "data": bus_data})


if __name__ == "__main__":
    app.run(debug=True)
