 WITH position_tracker_advanced__others AS (
         SELECT others."IMEI",
            time_bucket('00:00:01'::interval, last(others."createdAt", others."createdAt")) AS "timestamp",
            last(others."MainBat", others.iat) AS voltage,
            last(others."SIG_QUAL", others.iat) AS "RSSI",
            last(others."IGN_STATE", others.iat) AS ignition,
            last(others."CAN_STATE", others.iat) AS "OBD2"
           FROM others
          GROUP BY others."IMEI"
        ), battery_pack__18fff345 AS (
         SELECT "18fff345"."IMEI",
            time_bucket('00:00:01'::interval, last("18fff345"."timestamp", "18fff345"."timestamp")) AS "timestamp",
            last("18fff345"."T2B_TIn", "18fff345"."timestamp") AS inlet_temperature,
            last("18fff345"."T2B_TOut", "18fff345"."timestamp") AS outlet_temperature
           FROM "18fff345"
          WHERE "18fff345"."timestamp" < now()
          GROUP BY "18fff345"."IMEI"
        ), battery_pack__1012a1f3 AS (
         SELECT "1012a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1012a1f3"."timestamp", "1012a1f3"."timestamp")) AS "timestamp",
            last("1012a1f3"."B2V_MaxCellV1", "1012a1f3"."timestamp") AS "MaxCellV1",
            last("1012a1f3"."B2V_MinCellV1", "1012a1f3"."timestamp") AS "MinCellV1"
           FROM "1012a1f3"
          WHERE "1012a1f3"."timestamp" < now()
          GROUP BY "1012a1f3"."IMEI"
        ), battery_pack__1013a1f3 AS (
         SELECT "1013a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1013a1f3"."timestamp", "1013a1f3"."timestamp")) AS "timestamp",
            last("1013a1f3"."B2V_MaxCellV2", "1013a1f3"."timestamp") AS "MaxCellV2",
            last("1013a1f3"."B2V_MinCellV2", "1013a1f3"."timestamp") AS "MinCellV2"
           FROM "1013a1f3"
          WHERE "1013a1f3"."timestamp" < now()
          GROUP BY "1013a1f3"."IMEI"
        ), battery_pack__1014a1f3 AS (
         SELECT "1014a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1014a1f3"."timestamp", "1014a1f3"."timestamp")) AS "timestamp",
            last("1014a1f3"."B2V_MaxCellV3", "1014a1f3"."timestamp") AS "MaxCellV3",
            last("1014a1f3"."B2V_MinCellV3", "1014a1f3"."timestamp") AS "MinCellV3"
           FROM "1014a1f3"
          WHERE "1014a1f3"."timestamp" < now()
          GROUP BY "1014a1f3"."IMEI"
        ), battery_pack__1015a1f3 AS (
         SELECT "1015a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1015a1f3"."timestamp", "1015a1f3"."timestamp")) AS "timestamp",
            last("1015a1f3"."B2V_MaxCellV4", "1015a1f3"."timestamp") AS "MaxCellV4",
            last("1015a1f3"."B2V_MinCellV4", "1015a1f3"."timestamp") AS "MinCellV4"
           FROM "1015a1f3"
          WHERE "1015a1f3"."timestamp" < now()
          GROUP BY "1015a1f3"."IMEI"
        ), battery_pack__1016a1f3 AS (
         SELECT "1016a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1016a1f3"."timestamp", "1016a1f3"."timestamp")) AS "timestamp",
            last("1016a1f3"."B2V_MaxCellT1", "1016a1f3"."timestamp") AS "MaxCellT1",
            last("1016a1f3"."B2V_MinCellT1", "1016a1f3"."timestamp") AS "MinCellT1"
           FROM "1016a1f3"
          WHERE "1016a1f3"."timestamp" < now()
          GROUP BY "1016a1f3"."IMEI"
        ), battery_pack__1017a1f3 AS (
         SELECT "1017a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1017a1f3"."timestamp", "1017a1f3"."timestamp")) AS "timestamp",
            last("1017a1f3"."B2V_MaxCellT2", "1017a1f3"."timestamp") AS "MaxCellT2",
            last("1017a1f3"."B2V_MinCellT2", "1017a1f3"."timestamp") AS "MinCellT2"
           FROM "1017a1f3"
          WHERE "1017a1f3"."timestamp" < now()
          GROUP BY "1017a1f3"."IMEI"
        ), battery_pack__1018a1f3 AS (
         SELECT "1018a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1018a1f3"."timestamp", "1018a1f3"."timestamp")) AS "timestamp",
            last("1018a1f3"."B2V_MaxCellT3", "1018a1f3"."timestamp") AS "MaxCellT3",
            last("1018a1f3"."B2V_MinCellT3", "1018a1f3"."timestamp") AS "MinCellT3"
           FROM "1018a1f3"
          WHERE "1018a1f3"."timestamp" < now()
          GROUP BY "1018a1f3"."IMEI"
        ), battery_pack__1019a1f3 AS (
         SELECT "1019a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1019a1f3"."timestamp", "1019a1f3"."timestamp")) AS "timestamp",
            last("1019a1f3"."B2V_MaxCellT4", "1019a1f3"."timestamp") AS "MaxCellT4",
            last("1019a1f3"."B2V_MinCellT4", "1019a1f3"."timestamp") AS "MinCellT4"
           FROM "1019a1f3"
          WHERE "1019a1f3"."timestamp" < now()
          GROUP BY "1019a1f3"."IMEI"
        ), battery_pack__1820a1f3 AS (
         SELECT "1820a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1820a1f3"."timestamp", "1820a1f3"."timestamp")) AS "timestamp",
            last("1820a1f3"."B2V_SOC", "1820a1f3"."timestamp") AS "SoC"
           FROM "1820a1f3"
          WHERE "1820a1f3"."timestamp" < now()
          GROUP BY "1820a1f3"."IMEI"
        ), battery_pack__1821a1f3 AS (
         SELECT "1821a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1821a1f3"."timestamp", "1821a1f3"."timestamp")) AS "timestamp",
            last("1821a1f3"."B2V_SOH", "1821a1f3"."timestamp") AS "SoH"
           FROM "1821a1f3"
          WHERE "1821a1f3"."timestamp" < now()
          GROUP BY "1821a1f3"."IMEI"
        ), battery_pack__c00a1f3 AS (
         SELECT c00a1f3."IMEI",
            time_bucket('00:00:01'::interval, last(c00a1f3."timestamp", c00a1f3."timestamp")) AS "timestamp",
                CASE
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '0.000000'::text THEN 'Partially-Charged'::text
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '1.000000'::text THEN 'full-charged'::text
                    ELSE 'Unknown'::text
                END AS status__battery_pack__full_charge,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '0.000000'::text THEN 'Self-check'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '1.000000'::text THEN 'idle'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '2.000000'::text THEN 'charging'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text THEN 'discharging'::text
                    ELSE 'Unknown-BMS-Status'::text
                END AS status__battery_pack__current_polarity,
                CASE
                    WHEN last(c00a1f3."timestamp", c00a1f3."timestamp") < (now() - '01:00:00'::interval) THEN 'disconnected'::text
                    ELSE 'Online'::text
                END AS status__position_tracker_advanced__connection,
            last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") AS current,
            last(c00a1f3."B2V_HVP", c00a1f3."timestamp") AS voltage,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text AND last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") < 0::numeric THEN 'Regenerative-Braking'::text
                    ELSE 'Driving'::text
                END AS status__vehicle_control_unit__regenerative_braking
           FROM c00a1f3
          WHERE c00a1f3."timestamp" < now()
          GROUP BY c00a1f3."IMEI"
        ), battery_pack AS (
         SELECT battery_pack__c00a1f3."IMEI",
            battery_pack__c00a1f3."timestamp",
            battery_pack__c00a1f3.status__battery_pack__full_charge,
            battery_pack__c00a1f3.status__battery_pack__current_polarity,
            battery_pack__c00a1f3.status__position_tracker_advanced__connection,
            battery_pack__c00a1f3.current,
            battery_pack__c00a1f3.voltage,
            battery_pack__c00a1f3.status__vehicle_control_unit__regenerative_braking,
            battery_pack__1012a1f3."MaxCellV1",
            battery_pack__1012a1f3."MinCellV1",
            battery_pack__1012a1f3."MaxCellV1" - battery_pack__1012a1f3."MinCellV1" AS "DeltaCellV1",
            battery_pack__1013a1f3."MaxCellV2",
            battery_pack__1013a1f3."MinCellV2",
            battery_pack__1013a1f3."MaxCellV2" - battery_pack__1013a1f3."MinCellV2" AS "DeltaCellV2",
            battery_pack__1014a1f3."MaxCellV3",
            battery_pack__1014a1f3."MinCellV3",
            battery_pack__1014a1f3."MaxCellV3" - battery_pack__1014a1f3."MinCellV3" AS "DeltaCellV3",
            battery_pack__1015a1f3."MaxCellV4",
            battery_pack__1015a1f3."MinCellV4",
            battery_pack__1015a1f3."MaxCellV4" - battery_pack__1015a1f3."MinCellV4" AS "DeltaCellV4",
            battery_pack__1016a1f3."MaxCellT1",
            battery_pack__1016a1f3."MinCellT1",
            battery_pack__1017a1f3."MaxCellT2",
            battery_pack__1017a1f3."MinCellT2",
            battery_pack__1018a1f3."MaxCellT3",
            battery_pack__1018a1f3."MinCellT3",
            battery_pack__1019a1f3."MaxCellT4",
            battery_pack__1019a1f3."MinCellT4",
            battery_pack__1820a1f3."SoC",
            battery_pack__1821a1f3."SoH",
            battery_pack__18fff345.inlet_temperature,
            battery_pack__18fff345.outlet_temperature
           FROM battery_pack__c00a1f3
             FULL JOIN battery_pack__1820a1f3 ON battery_pack__1820a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1821a1f3 ON battery_pack__1821a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1012a1f3 ON battery_pack__1012a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1013a1f3 ON battery_pack__1013a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1014a1f3 ON battery_pack__1014a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1015a1f3 ON battery_pack__1015a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1016a1f3 ON battery_pack__1016a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1017a1f3 ON battery_pack__1017a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1018a1f3 ON battery_pack__1018a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__1019a1f3 ON battery_pack__1019a1f3."IMEI" = battery_pack__c00a1f3."IMEI"
             FULL JOIN battery_pack__18fff345 ON battery_pack__18fff345."IMEI" = battery_pack__c00a1f3."IMEI"
        ), vehicle AS (
         SELECT
                CASE
                    WHEN battery_pack."IMEI" IS NOT NULL THEN battery_pack."IMEI"
                    ELSE tc_devices.uniqueid::bigint
                END AS "IMEI",
                CASE
                    WHEN battery_pack."timestamp" IS NULL THEN '2022-02-03 01:52:22+05:30'::timestamp with time zone
                    ELSE battery_pack."timestamp"
                END AS "timestamp",
                CASE
                    WHEN battery_pack.status__position_tracker_advanced__connection = 'Online'::text THEN array_remove(ARRAY[battery_pack.status__battery_pack__full_charge, battery_pack.status__battery_pack__current_polarity, battery_pack.status__position_tracker_advanced__connection, battery_pack.status__vehicle_control_unit__regenerative_braking], NULL::text)
                    WHEN battery_pack."timestamp" IS NULL THEN array_remove(ARRAY['Offline'::text], NULL::text)
                    ELSE array_remove(ARRAY[battery_pack.status__position_tracker_advanced__connection], NULL::text)
                END AS status,
            battery_pack.current,
            battery_pack.voltage,
            battery_pack."SoC",
            battery_pack."SoH",
            battery_pack."MaxCellV1",
            battery_pack."MaxCellV2",
            battery_pack."MaxCellV3",
            battery_pack."MaxCellV4",
            battery_pack."MinCellV1",
            battery_pack."MinCellV2",
            battery_pack."MinCellV3",
            battery_pack."MinCellV4",
            battery_pack."DeltaCellV1",
            battery_pack."DeltaCellV2",
            battery_pack."DeltaCellV3",
            battery_pack."DeltaCellV4",
            battery_pack."MaxCellT1",
            battery_pack."MaxCellT2",
            battery_pack."MaxCellT3",
            battery_pack."MaxCellT4",
            battery_pack."MinCellT1",
            battery_pack."MinCellT2",
            battery_pack."MinCellT3",
            battery_pack."MinCellT4",
            battery_pack.inlet_temperature,
            battery_pack.outlet_temperature,
            tc_devices.name,
            tc_devices.attributes::jsonb ->> 'depot'::text AS depot,
            tc_devices.attributes::jsonb ->> 'city'::text AS city,
            tc_devices.id
           FROM battery_pack
             FULL JOIN traccar.tc_devices ON tc_devices.uniqueid::text = battery_pack."IMEI"::text
          WHERE tc_devices.uniqueid::text <> tc_devices.name::text
        ), position_tracker_advanced AS (
         SELECT tc_positions.deviceid,
            last(tc_positions.fixtime, tc_positions.fixtime) + '05:30:00'::interval AS "timestamp",
            last(tc_positions.latitude, tc_positions.fixtime)::text AS latitude,
            last(tc_positions.longitude, tc_positions.fixtime)::text AS longitude,
            last(tc_positions.speed, tc_positions.fixtime)::text AS speed
           FROM traccar.tc_positions
          WHERE tc_positions.latitude <> 0::double precision AND tc_positions.longitude <> 0::double precision
          GROUP BY tc_positions.deviceid
        ), asset AS (
         SELECT row_number() OVER (ORDER BY vehicle."IMEI") AS "#",
            now() AS "updatedAt",
            vehicle."IMEI",
            vehicle."timestamp" AS timestamp_e,
            vehicle.status,
            vehicle.name,
            vehicle.depot,
            vehicle.city,
            vehicle.voltage,
            vehicle.current,
            vehicle."SoC",
            vehicle."SoH",
            vehicle."MaxCellV1",
            vehicle."MaxCellV2",
            vehicle."MaxCellV3",
            vehicle."MaxCellV4",
            vehicle."MinCellV1",
            vehicle."MinCellV2",
            vehicle."MinCellV3",
            vehicle."MinCellV4",
            vehicle."DeltaCellV1",
            vehicle."DeltaCellV2",
            vehicle."DeltaCellV3",
            vehicle."DeltaCellV4",
            vehicle."MaxCellT1",
            vehicle."MaxCellT2",
            vehicle."MaxCellT3",
            vehicle."MaxCellT4",
            vehicle."MinCellT1",
            vehicle."MinCellT2",
            vehicle."MinCellT3",
            vehicle."MinCellT4",
            vehicle.inlet_temperature,
            vehicle.outlet_temperature,
            position_tracker_advanced."timestamp" AS timestamp_g,
            position_tracker_advanced.latitude,
            position_tracker_advanced.longitude,
            position_tracker_advanced.speed,
            position_tracker_advanced__others.voltage AS "24V",
            position_tracker_advanced__others."RSSI",
            position_tracker_advanced__others.ignition,
            position_tracker_advanced__others."OBD2"
           FROM vehicle
             LEFT JOIN position_tracker_advanced ON position_tracker_advanced.deviceid = vehicle.id
             LEFT JOIN position_tracker_advanced__others ON position_tracker_advanced__others."IMEI" = vehicle."IMEI"
        ), bus_battery_data_cte AS (
         SELECT asset."IMEI" AS imei,
            asset.status,
            replace(asset.name::text, ' '::text, ''::text) AS name,
            asset.depot,
            asset.city,
            asset.latitude,
            asset.longitude,
            asset."SoC" AS soc,
            asset."SoH" AS soh,
            asset.voltage,
            asset.current AS total_current,
            asset.inlet_temperature,
            asset.outlet_temperature,
            asset."MaxCellV1" AS max_cell_v1,
            asset."MaxCellV2" AS max_cell_v2,
            asset."MaxCellV3" AS max_cell_v3,
            asset."MaxCellV4" AS max_cell_v4,
            asset."MinCellV1" AS min_cell_v1,
            asset."MinCellV2" AS min_cell_v2,
            asset."MinCellV3" AS min_cell_v3,
            asset."MinCellV4" AS min_cell_v4,
            asset."DeltaCellV1" AS delta_cell_v1,
            asset."DeltaCellV2" AS delta_cell_v2,
            asset."DeltaCellV3" AS delta_cell_v3,
            asset."DeltaCellV4" AS delta_cell_v4,
            asset."MaxCellT1" AS max_cell_t1,
            asset."MaxCellT2" AS max_cell_t2,
            asset."MaxCellT3" AS max_cell_t3,
            asset."MaxCellT4" AS max_cell_t4,
            asset."MinCellT1" AS min_cell_t1,
            asset."MinCellT2" AS min_cell_t2,
            asset."MinCellT3" AS min_cell_t3,
            asset."MinCellT4" AS min_cell_t4,
            asset.speed
           FROM asset
        )
 SELECT bus_battery_data_cte.imei,
    bus_battery_data_cte.status,
    bus_battery_data_cte.name,
    bus_battery_data_cte.depot,
    bus_battery_data_cte.city,
    bus_battery_data_cte.latitude,
    bus_battery_data_cte.longitude,
    bus_battery_data_cte.soc,
    bus_battery_data_cte.soh,
    bus_battery_data_cte.voltage,
    bus_battery_data_cte.total_current,
    bus_battery_data_cte.inlet_temperature,
    bus_battery_data_cte.outlet_temperature,
    bus_battery_data_cte.max_cell_v1,
    bus_battery_data_cte.max_cell_v2,
    bus_battery_data_cte.max_cell_v3,
    bus_battery_data_cte.max_cell_v4,
    bus_battery_data_cte.min_cell_v1,
    bus_battery_data_cte.min_cell_v2,
    bus_battery_data_cte.min_cell_v3,
    bus_battery_data_cte.min_cell_v4,
    bus_battery_data_cte.delta_cell_v1,
    bus_battery_data_cte.delta_cell_v2,
    bus_battery_data_cte.delta_cell_v3,
    bus_battery_data_cte.delta_cell_v4,
    bus_battery_data_cte.max_cell_t1,
    bus_battery_data_cte.max_cell_t2,
    bus_battery_data_cte.max_cell_t3,
    bus_battery_data_cte.max_cell_t4,
    bus_battery_data_cte.min_cell_t1,
    bus_battery_data_cte.min_cell_t2,
    bus_battery_data_cte.min_cell_t3,
    bus_battery_data_cte.min_cell_t4,
    bus_battery_data_cte.speed
   FROM bus_battery_data_cte;