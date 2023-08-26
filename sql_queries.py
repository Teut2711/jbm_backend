bus_data_cte = """
WITH
"battery_pack" AS (
    SELECT
        "IMEI",
        LAST("timestamp", "timestamp") AS "timestamp",
        CASE
            WHEN LAST("B2V_FullChrg", "timestamp") = '0.000000' THEN 'Partially-Charged'
            WHEN LAST("B2V_FullChrg", "timestamp") = '1.000000' THEN 'full-charged'
            ELSE 'Unknown'
        END "status__battery_pack__full_charge",
        CASE
            WHEN LAST("B2V_BMSSta", "timestamp") = '0.000000' THEN 'Self-check'
            WHEN LAST("B2V_BMSSta", "timestamp") = '1.000000' THEN 'idle'
            WHEN LAST("B2V_BMSSta", "timestamp") = '2.000000' THEN 'charging'
            WHEN LAST("B2V_BMSSta", "timestamp") = '3.000000' THEN 'discharging'
            ELSE 'Unknown-BMS-Status'
        END "status__battery_pack__current_polarity",
        CASE
            WHEN LAST("timestamp", "timestamp") < NOW() - '1 HOUR'::INTERVAL THEN 'disconnected'
            WHEN LAST("timestamp", "timestamp") > NOW() THEN 'Error'
            ELSE 'Online'
        END "status__position_tracker_advanced__connection"
    FROM "c00a1f3"
    GROUP BY "IMEI"
),
"vehicle" AS (
    SELECT
        CASE
            WHEN "battery_pack"."IMEI" IS NOT NULL THEN "battery_pack"."IMEI"
            ELSE "traccar"."tc_devices"."uniqueid"::BIGINT
        END "IMEI",
        CASE
            WHEN "battery_pack"."timestamp" IS NULL THEN '2022-02-02 22:22:22+02:00'::TIMESTAMP WITH TIME ZONE
            ELSE "battery_pack"."timestamp"
        END "timestamp",
        CASE
            WHEN "battery_pack"."status__position_tracker_advanced__connection" = 'Online' THEN ARRAY_REMOVE(ARRAY["battery_pack"."status__battery_pack__full_charge", "battery_pack"."status__battery_pack__current_polarity", "battery_pack"."status__position_tracker_advanced__connection"], NULL)
            WHEN "battery_pack"."timestamp" IS NULL THEN ARRAY_REMOVE(ARRAY['{Waiting}'], NULL)
            ELSE ARRAY_REMOVE(ARRAY["battery_pack"."status__position_tracker_advanced__connection"], NULL)
        END "status",
        "traccar"."tc_devices"."name",
        ("traccar"."tc_devices"."attributes"::JSONB)->>'depot' AS "depot",
        ("traccar"."tc_devices"."attributes"::JSONB)->>'city' AS "city",
        "traccar"."tc_devices"."id"
    FROM "battery_pack"
    FULL JOIN "traccar"."tc_devices" ON "traccar"."tc_devices"."uniqueid"::TEXT = "battery_pack"."IMEI"::TEXT
    WHERE "traccar"."tc_devices"."uniqueid"::TEXT != "traccar"."tc_devices"."name"::TEXT
),
"position_tracker_advanced" AS (
    SELECT
        "deviceid",
        LAST("fixtime", "fixtime") + '5 HOUR 30 MINUTE'::INTERVAL AS "timestamp",
        LAST("latitude", "fixtime")::TEXT AS "latitude",
        LAST("longitude", "fixtime")::TEXT AS "longitude"
    FROM "traccar"."tc_positions"
    WHERE
        "latitude" != 0 AND
        "longitude" != 0
    GROUP BY "deviceid"
), 
BUS_DATA AS (
SELECT
    ROW_NUMBER() OVER (ORDER BY "IMEI" ASC) AS "#",
    NOW() AS "updatedAt",
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
LEFT OUTER JOIN "position_tracker_advanced" ON "position_tracker_advanced"."deviceid" = "vehicle"."id"),
VoltageOrdered AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_HVP" AS "voltage"
  FROM "c00a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),

CurrentOrdered AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_TotalI" AS "total_current"
  FROM "c00a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),

SOCOrdered AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_SOC" AS "soc"
  FROM "1820a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),

SOHOrdered AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_SOH" AS "soh"
  FROM "1821a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),
AverageTemperature AS (
    SELECT DISTINCT ON ("IMEI") "IMEI",
      "B2T_TAvg"::NUMERIC AS "average_temperature",
      "timestamp" AS "average_temperature_time"
    FROM
      "18ff45f3"
    ORDER BY
      "IMEI", "timestamp" DESC
  ),
MinCellT1 AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_MinCellT1" AS "min_cell_t1"
  FROM "1016a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),

MaxCellT1 AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_MaxCellT1" AS "max_cell_t1"
  FROM "1016a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),

MinCellV1 AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_MinCellV1" AS "min_cell_v1"
  FROM "1012a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),

MaxCellV1 AS (
  SELECT DISTINCT ON ("IMEI") "IMEI", "B2V_MaxCellV1" AS "max_cell_v1"
  FROM "1012a1f3"
  ORDER BY "IMEI", "timestamp" DESC
),
BUS_BATTERY AS (
SELECT
  v."IMEI" AS imei,
  voltage,
  total_current,
  soc,
  soh,
  avt."average_temperature",
  mt1."min_cell_t1",
  mt2."max_cell_t1",
  mv1."min_cell_v1",
  mv2."max_cell_v1"
FROM
  VoltageOrdered v
INNER JOIN
  CurrentOrdered c ON v."IMEI" = c."IMEI"
INNER JOIN
  SOCOrdered s ON v."IMEI" = s."IMEI"
INNER JOIN
  SOHOrdered h ON v."IMEI" = h."IMEI"
LEFT JOIN
  AverageTemperature avt ON v."IMEI" = avt."IMEI"

	LEFT JOIN
  MinCellT1 mt1 ON v."IMEI" = mt1."IMEI"
LEFT JOIN
  MaxCellT1 mt2 ON v."IMEI" = mt2."IMEI"
LEFT JOIN
  MinCellV1 mv1 ON v."IMEI" = mv1."IMEI"
LEFT JOIN
  MaxCellV1 mv2 ON v."IMEI" = mv2."IMEI"
)
,
BUS_BATTERY_DATA AS (

SELECT "IMEI" AS imei,  status, name, depot, city, latitude, longitude, soc, soh, voltage, total_current,average_temperature AS temperature,
	min_cell_t1 AS min_cell_temp,
  max_cell_t1 AS max_cell_temp,
  min_cell_v1 AS min_cell_volt,
  max_cell_v1 AS max_cell_volt
	FROM BUS_DATA bd LEFT JOIN BUS_BATTERY bb ON bd."IMEI"= bb.imei
	)
"""
