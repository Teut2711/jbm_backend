WITH
"position_tracker_advanced__others" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("createdAt", "createdAt")) AS "timestamp",
        LAST("MainBat", "iat") AS "voltage",
        LAST("SIG_QUAL", "iat") AS "RSSI",
        LAST("IGN_STATE", "iat") AS "ignition",
        LAST("CAN_STATE", "iat") AS "OBD2"
    FROM "others"
    GROUP BY "IMEI"
),
"battery_pack__18fff345" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("T2B_TIn", "timestamp") AS "inlet_temperature",
        LAST("T2B_TOut", "timestamp") AS "outlet_temperature"
    FROM "18fff345"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1012a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellV1", "timestamp") AS "MaxCellV1",
        LAST("B2V_MinCellV1", "timestamp") AS "MinCellV1"
    FROM "1012a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1013a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellV2", "timestamp") AS "MaxCellV2",
        LAST("B2V_MinCellV2", "timestamp") AS "MinCellV2"
    FROM "1013a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1014a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellV3", "timestamp") AS "MaxCellV3",
        LAST("B2V_MinCellV3", "timestamp") AS "MinCellV3"
    FROM "1014a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1015a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellV4", "timestamp") AS "MaxCellV4",
        LAST("B2V_MinCellV4", "timestamp") AS "MinCellV4"
    FROM "1015a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1016a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellT1", "timestamp") AS "MaxCellT1",
        LAST("B2V_MinCellT1", "timestamp") AS "MinCellT1"
    FROM "1016a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1017a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellT2", "timestamp") AS "MaxCellT2",
        LAST("B2V_MinCellT2", "timestamp") AS "MinCellT2"
    FROM "1017a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1018a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellT3", "timestamp") AS "MaxCellT3",
        LAST("B2V_MinCellT3", "timestamp") AS "MinCellT3"
    FROM "1018a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1019a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_MaxCellT4", "timestamp") AS "MaxCellT4",
        LAST("B2V_MinCellT4", "timestamp") AS "MinCellT4"
    FROM "1019a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1820a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_SOC", "timestamp") AS "SoC"
    FROM "1820a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__1821a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
        LAST("B2V_SOH", "timestamp") AS "SoH"
    FROM "1821a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack__c00a1f3" AS (
    SELECT
        "IMEI",
        TIME_BUCKET('1 SECOND', LAST("timestamp", "timestamp")) AS "timestamp",
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
            ELSE 'Online'
        END "status__position_tracker_advanced__connection",
        LAST("B2V_TotalI", "timestamp") AS "current",
        LAST("B2V_HVP", "timestamp") AS "voltage",
        CASE
            WHEN LAST("B2V_BMSSta", "timestamp") = '3.000000' AND LAST("B2V_TotalI", "timestamp")::NUMERIC < 0 THEN 'Regenerative-Braking'
            ELSE 'Driving'
        END "status__vehicle_control_unit__regenerative_braking"
    FROM "c00a1f3"
    WHERE "timestamp" < NOW()
    GROUP BY "IMEI"
),
"battery_pack" AS (
    SELECT
        "battery_pack__c00a1f3".*,
        "battery_pack__1012a1f3"."MaxCellV1",
        "battery_pack__1012a1f3"."MinCellV1",
        "battery_pack__1012a1f3"."MaxCellV1" - "battery_pack__1012a1f3"."MinCellV1" AS "DeltaCellV1",
        "battery_pack__1013a1f3"."MaxCellV2",
        "battery_pack__1013a1f3"."MinCellV2",
        "battery_pack__1013a1f3"."MaxCellV2" - "battery_pack__1013a1f3"."MinCellV2" AS "DeltaCellV2",
        "battery_pack__1014a1f3"."MaxCellV3",
        "battery_pack__1014a1f3"."MinCellV3",
        "battery_pack__1014a1f3"."MaxCellV3" - "battery_pack__1014a1f3"."MinCellV3" AS "DeltaCellV3",
        "battery_pack__1015a1f3"."MaxCellV4",
        "battery_pack__1015a1f3"."MinCellV4",
        "battery_pack__1015a1f3"."MaxCellV4" - "battery_pack__1015a1f3"."MinCellV4" AS "DeltaCellV4",
        "battery_pack__1016a1f3"."MaxCellT1",
        "battery_pack__1016a1f3"."MinCellT1",
        "battery_pack__1017a1f3"."MaxCellT2",
        "battery_pack__1017a1f3"."MinCellT2",
        "battery_pack__1018a1f3"."MaxCellT3",
        "battery_pack__1018a1f3"."MinCellT3",
        "battery_pack__1019a1f3"."MaxCellT4",
        "battery_pack__1019a1f3"."MinCellT4",
        "battery_pack__1820a1f3"."SoC",
        "battery_pack__1821a1f3"."SoH",
        "battery_pack__18fff345"."inlet_temperature",
        "battery_pack__18fff345"."outlet_temperature"
    FROM "battery_pack__c00a1f3"
    FULL JOIN "battery_pack__1820a1f3" ON "battery_pack__1820a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1821a1f3" ON "battery_pack__1821a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1012a1f3" ON "battery_pack__1012a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1013a1f3" ON "battery_pack__1013a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1014a1f3" ON "battery_pack__1014a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1015a1f3" ON "battery_pack__1015a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1016a1f3" ON "battery_pack__1016a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1017a1f3" ON "battery_pack__1017a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1018a1f3" ON "battery_pack__1018a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__1019a1f3" ON "battery_pack__1019a1f3"."IMEI" = "battery_pack__c00a1f3"."IMEI"
    FULL JOIN "battery_pack__18fff345" ON "battery_pack__18fff345"."IMEI" = "battery_pack__c00a1f3"."IMEI"
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
            WHEN "battery_pack"."status__position_tracker_advanced__connection" = 'Online' THEN ARRAY_REMOVE(ARRAY["battery_pack"."status__battery_pack__full_charge", "battery_pack"."status__battery_pack__current_polarity", "battery_pack"."status__position_tracker_advanced__connection", "status__vehicle_control_unit__regenerative_braking"], NULL)
            WHEN "battery_pack"."timestamp" IS NULL THEN ARRAY_REMOVE(ARRAY['Offline'], NULL)
            ELSE ARRAY_REMOVE(ARRAY["battery_pack"."status__position_tracker_advanced__connection"], NULL)
        END "status",
        "battery_pack"."current",
        "battery_pack"."voltage",
        "battery_pack"."SoC",
        "battery_pack"."SoH",
        "battery_pack"."MaxCellV1",
        "battery_pack"."MaxCellV2",
        "battery_pack"."MaxCellV3",
        "battery_pack"."MaxCellV4",
        "battery_pack"."MinCellV1",
        "battery_pack"."MinCellV2",
        "battery_pack"."MinCellV3",
        "battery_pack"."MinCellV4",
        "battery_pack"."DeltaCellV1",
        "battery_pack"."DeltaCellV2",
        "battery_pack"."DeltaCellV3",
        "battery_pack"."DeltaCellV4",
        "battery_pack"."MaxCellT1",
        "battery_pack"."MaxCellT2",
        "battery_pack"."MaxCellT3",
        "battery_pack"."MaxCellT4",
        "battery_pack"."MinCellT1",
        "battery_pack"."MinCellT2",
        "battery_pack"."MinCellT3",
        "battery_pack"."MinCellT4",
        "battery_pack"."inlet_temperature",
        "battery_pack"."outlet_temperature",
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
        LAST("longitude", "fixtime")::TEXT AS "longitude",
        LAST("speed", "fixtime")::TEXT AS "speed"
    FROM "traccar"."tc_positions"
    WHERE
        "latitude" != 0 AND
        "longitude" != 0
    GROUP BY "deviceid"
),
"asset" AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY "vehicle"."IMEI" ASC) AS "#",
        NOW() AS "updatedAt",
        "vehicle"."IMEI",
        "vehicle"."timestamp" AS "timestamp_e",
        "vehicle"."status",
        "vehicle"."name",
        "vehicle"."depot",
        "vehicle"."city",
        "vehicle"."voltage",
        "vehicle"."current",
        "vehicle"."SoC",
        "vehicle"."SoH",
        "vehicle"."MaxCellV1",
        "vehicle"."MaxCellV2",
        "vehicle"."MaxCellV3",
        "vehicle"."MaxCellV4",
        "vehicle"."MinCellV1",
        "vehicle"."MinCellV2",
        "vehicle"."MinCellV3",
        "vehicle"."MinCellV4",
        "vehicle"."DeltaCellV1",
        "vehicle"."DeltaCellV2",
        "vehicle"."DeltaCellV3",
        "vehicle"."DeltaCellV4",
        "vehicle"."MaxCellT1",
        "vehicle"."MaxCellT2",
        "vehicle"."MaxCellT3",
        "vehicle"."MaxCellT4",
        "vehicle"."MinCellT1",
        "vehicle"."MinCellT2",
        "vehicle"."MinCellT3",
        "vehicle"."MinCellT4",
        "vehicle"."inlet_temperature",
        "vehicle"."outlet_temperature",
        "position_tracker_advanced"."timestamp" AS "timestamp_g",
        "position_tracker_advanced"."latitude",
        "position_tracker_advanced"."longitude",
        "position_tracker_advanced"."speed",
        "position_tracker_advanced__others"."voltage" AS "24V",
        "position_tracker_advanced__others"."RSSI",
        "position_tracker_advanced__others"."ignition",
        "position_tracker_advanced__others"."OBD2"
    FROM "vehicle"
    LEFT OUTER JOIN "position_tracker_advanced" ON "position_tracker_advanced"."deviceid" = "vehicle"."id"
    LEFT OUTER JOIN "position_tracker_advanced__others" ON "position_tracker_advanced__others"."IMEI" = "vehicle"."IMEI"
),
BUS_BATTERY_DATA_CTE AS (
SELECT
    "IMEI" AS imei,
    status,
    name,
    depot,
    city,
    latitude,
    longitude,
    "SoC" AS soc,
    "SoH" AS soh,
    voltage,
    current AS total_current,
    inlet_temperature,
    outlet_temperature,
    "MaxCellV1" AS max_cell_v1,
    "MaxCellV2" AS max_cell_v2,
    "MaxCellV3" AS max_cell_v3,
    "MaxCellV4" AS max_cell_v4,
    "MinCellV1" AS min_cell_v1,
    "MinCellV2" AS min_cell_v2,
    "MinCellV3" AS min_cell_v3,
    "MinCellV4" AS min_cell_v4,
    "DeltaCellV1" AS delta_cell_v1,
    "DeltaCellV2" AS delta_cell_v2,
    "DeltaCellV3" AS delta_cell_v3,
    "DeltaCellV4" AS delta_cell_v4,
    "MaxCellT1" AS max_cell_t1,
    "MaxCellT2" AS max_cell_t2,
    "MaxCellT3" AS max_cell_t3,
    "MaxCellT4" AS max_cell_t4,
    "MinCellT1" AS min_cell_t1,
    "MinCellT2" AS min_cell_t2,
    "MinCellT3" AS min_cell_t3,
    "MinCellT4" AS min_cell_t4,
    speed
FROM "asset"
)  
