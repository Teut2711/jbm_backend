bus_data_cte = """
WITH
"battery_pack" AS (
	SELECT
		"IMEI",
		LAST("timestamp", "timestamp") AS "timestamp",
		CASE
			WHEN LAST("B2V_FullChrg", "timestamp") = '1.000000' THEN 'full-charged'
		END "status__full_charged",
		CASE
			WHEN LAST("B2V_BMSSta", "timestamp") = '0.000000' THEN 'Self-check'
			WHEN LAST("B2V_BMSSta", "timestamp") = '1.000000' THEN 'idle'
			WHEN LAST("B2V_BMSSta", "timestamp") = '2.000000' THEN 'charging'
			WHEN LAST("B2V_BMSSta", "timestamp") = '3.000000' THEN 'discharging'
		END "status__BMS"
	FROM "c00a1f3"
	GROUP BY "IMEI"
),
"vehicle" AS (
	SELECT
		"battery_pack"."IMEI",
		"battery_pack"."timestamp",
		ARRAY_REMOVE(ARRAY["battery_pack"."status__full_charged", "battery_pack"."status__BMS"], NULL) AS "status",
		"traccar"."tc_devices"."name",
		("traccar"."tc_devices"."attributes"::JSONB)->>'depot' AS "depot",
		("traccar"."tc_devices"."attributes"::JSONB)->>'city' AS "city",
		"traccar"."tc_devices"."id"
	FROM "battery_pack"
	LEFT OUTER JOIN "traccar"."tc_devices" ON "traccar"."tc_devices"."uniqueid"::TEXT = "battery_pack"."IMEI"::TEXT
),
"position_tracker_advanced" AS (
	SELECT
		"deviceid",
		LAST("fixtime", "fixtime") AS "timestamp",
		LAST("latitude", "fixtime") AS "latitude",
		LAST("longitude", "fixtime") AS "longitude"
	FROM "traccar"."tc_positions"
	WHERE
		"latitude" != 0 AND
		"longitude" != 0
	GROUP BY "deviceid"
), 
BUS_DATA AS (
    SELECT
	"vehicle"."IMEI",
	"vehicle"."timestamp" AS "timestamp_e",
	"vehicle"."status",
	"vehicle"."name",
	"vehicle"."depot",
	"vehicle"."city",
	"position_tracker_advanced"."timestamp" AS "timestamp_g",
	"position_tracker_advanced"."latitude",
	"position_tracker_advanced"."longitude"
FROM "vehicle"
LEFT OUTER JOIN "position_tracker_advanced" ON "position_tracker_advanced"."deviceid" = "vehicle"."id"
)
"""
