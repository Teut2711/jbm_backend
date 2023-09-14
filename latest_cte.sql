 WITH position_tracker_advanced__others AS (
         SELECT others."IMEI",
            time_bucket('00:00:01'::interval, last(others."createdAt", others."createdAt")) AS "timestamp",
            last(others."MainBat", others.iat) AS "24V",
            last(others."SIG_QUAL", others.iat) AS "RSSI",
            last(others."IGN_STATE", others.iat) AS ignition,
            last(others."CAN_STATE", others.iat) AS "OBD2",
            last(others.sleep_mode, others.iat) AS sleep_mode
           FROM others
          GROUP BY others."IMEI"
        ), position_tracker_advanced__positions AS (
         SELECT tc_positions.deviceid,
            last(tc_positions.fixtime, tc_positions.fixtime) + '05:30:00'::interval AS "timestamp",
            last(tc_positions.latitude, tc_positions.fixtime)::text AS latitude,
            last(tc_positions.longitude, tc_positions.fixtime)::text AS longitude,
            last(tc_positions.speed, tc_positions.fixtime)::text AS speed
           FROM traccar.tc_positions
          WHERE tc_positions.latitude <> 0::double precision AND tc_positions.longitude <> 0::double precision
          GROUP BY tc_positions.deviceid
        ), position_tracker_advanced__devices AS (
         SELECT tc_devices.uniqueid AS "IMEI",
            tc_devices.name,
            initcap(tc_devices.attributes::jsonb ->> 'depot'::text) AS depot,
            initcap(tc_devices.attributes::jsonb ->> 'city'::text) AS city,
            tc_devices.id
           FROM traccar.tc_devices
          WHERE tc_devices.uniqueid::text <> tc_devices.name::text
        ), position_tracker_advanced AS (
         SELECT position_tracker_advanced__devices."IMEI",
            position_tracker_advanced__devices.name,
            position_tracker_advanced__devices.depot,
            position_tracker_advanced__devices.city,
            position_tracker_advanced__others."timestamp" AS position_tracker_advanced__others__timestamp,
            position_tracker_advanced__positions."timestamp" AS position_tracker_advanced__positions__timestamp,
                CASE
                    WHEN position_tracker_advanced__others."timestamp" < (now() - '00:35:00'::interval) AND position_tracker_advanced__positions."timestamp" < (now() - '00:35:00'::interval) THEN 'position_tracker_advanced__offline'::text
                    ELSE 'position_tracker_advanced__online'::text
                END AS position_tracker_advanced__status,
            position_tracker_advanced__others."24V",
            position_tracker_advanced__others."RSSI",
            position_tracker_advanced__others.ignition,
            position_tracker_advanced__others."OBD2",
            position_tracker_advanced__others.sleep_mode,
            position_tracker_advanced__positions.latitude,
            position_tracker_advanced__positions.longitude,
            position_tracker_advanced__positions.speed
           FROM position_tracker_advanced__devices
             LEFT JOIN position_tracker_advanced__positions ON position_tracker_advanced__positions.deviceid = position_tracker_advanced__devices.id
             LEFT JOIN position_tracker_advanced__others ON position_tracker_advanced__others."IMEI" = position_tracker_advanced__devices."IMEI"::bigint
        ), "vehicle__CAN_frames" AS (
         SELECT "CAN_frames"."IMEI",
            time_bucket('00:00:01'::interval, last("CAN_frames"."timestamp", "CAN_frames"."timestamp")) AS "timestamp"
           FROM "CAN_frames"
          WHERE "CAN_frames"."timestamp" < now()
          GROUP BY "CAN_frames"."IMEI"
        ), vehicle__18fff345 AS (
         SELECT "18fff345"."IMEI",
            last("18fff345"."T2B_TIn", "18fff345"."timestamp") AS inlet_temperature,
            last("18fff345"."T2B_TOut", "18fff345"."timestamp") AS outlet_temperature
           FROM "18fff345"
          WHERE "18fff345"."timestamp" < now()
          GROUP BY "18fff345"."IMEI"
        ), vehicle__1012a1f3 AS (
         SELECT "1012a1f3"."IMEI",
            last("1012a1f3"."B2V_MaxCellV1", "1012a1f3"."timestamp") AS "MaxCellV1",
            last("1012a1f3"."B2V_MinCellV1", "1012a1f3"."timestamp") AS "MinCellV1"
           FROM "1012a1f3"
          WHERE "1012a1f3"."timestamp" < now()
          GROUP BY "1012a1f3"."IMEI"
        ), vehicle__1013a1f3 AS (
         SELECT "1013a1f3"."IMEI",
            last("1013a1f3"."B2V_MaxCellV2", "1013a1f3"."timestamp") AS "MaxCellV2",
            last("1013a1f3"."B2V_MinCellV2", "1013a1f3"."timestamp") AS "MinCellV2"
           FROM "1013a1f3"
          WHERE "1013a1f3"."timestamp" < now()
          GROUP BY "1013a1f3"."IMEI"
        ), vehicle__1014a1f3 AS (
         SELECT "1014a1f3"."IMEI",
            last("1014a1f3"."B2V_MaxCellV3", "1014a1f3"."timestamp") AS "MaxCellV3",
            last("1014a1f3"."B2V_MinCellV3", "1014a1f3"."timestamp") AS "MinCellV3"
           FROM "1014a1f3"
          WHERE "1014a1f3"."timestamp" < now()
          GROUP BY "1014a1f3"."IMEI"
        ), vehicle__1015a1f3 AS (
         SELECT "1015a1f3"."IMEI",
            last("1015a1f3"."B2V_MaxCellV4", "1015a1f3"."timestamp") AS "MaxCellV4",
            last("1015a1f3"."B2V_MinCellV4", "1015a1f3"."timestamp") AS "MinCellV4"
           FROM "1015a1f3"
          WHERE "1015a1f3"."timestamp" < now()
          GROUP BY "1015a1f3"."IMEI"
        ), vehicle__1016a1f3 AS (
         SELECT "1016a1f3"."IMEI",
            last("1016a1f3"."B2V_MaxCellT1", "1016a1f3"."timestamp") AS "MaxCellT1",
            last("1016a1f3"."B2V_MinCellT1", "1016a1f3"."timestamp") AS "MinCellT1"
           FROM "1016a1f3"
          WHERE "1016a1f3"."timestamp" < now()
          GROUP BY "1016a1f3"."IMEI"
        ), vehicle__1017a1f3 AS (
         SELECT "1017a1f3"."IMEI",
            last("1017a1f3"."B2V_MaxCellT2", "1017a1f3"."timestamp") AS "MaxCellT2",
            last("1017a1f3"."B2V_MinCellT2", "1017a1f3"."timestamp") AS "MinCellT2"
           FROM "1017a1f3"
          WHERE "1017a1f3"."timestamp" < now()
          GROUP BY "1017a1f3"."IMEI"
        ), vehicle__1018a1f3 AS (
         SELECT "1018a1f3"."IMEI",
            last("1018a1f3"."B2V_MaxCellT3", "1018a1f3"."timestamp") AS "MaxCellT3",
            last("1018a1f3"."B2V_MinCellT3", "1018a1f3"."timestamp") AS "MinCellT3"
           FROM "1018a1f3"
          WHERE "1018a1f3"."timestamp" < now()
          GROUP BY "1018a1f3"."IMEI"
        ), vehicle__1019a1f3 AS (
         SELECT "1019a1f3"."IMEI",
            last("1019a1f3"."B2V_MaxCellT4", "1019a1f3"."timestamp") AS "MaxCellT4",
            last("1019a1f3"."B2V_MinCellT4", "1019a1f3"."timestamp") AS "MinCellT4"
           FROM "1019a1f3"
          WHERE "1019a1f3"."timestamp" < now()
          GROUP BY "1019a1f3"."IMEI"
        ), vehicle__1820a1f3 AS (
         SELECT "1820a1f3"."IMEI",
            last("1820a1f3"."B2V_SOC", "1820a1f3"."timestamp") AS "SoC"
           FROM "1820a1f3"
          WHERE "1820a1f3"."timestamp" < now()
          GROUP BY "1820a1f3"."IMEI"
        ), vehicle__1821a1f3 AS (
         SELECT "1821a1f3"."IMEI",
            last("1821a1f3"."B2V_SOH", "1821a1f3"."timestamp") AS "SoH"
           FROM "1821a1f3"
          WHERE "1821a1f3"."timestamp" < now()
          GROUP BY "1821a1f3"."IMEI"
        ), vehicle__c00a1f3 AS (
         SELECT c00a1f3."IMEI",
            last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") AS current,
            last(c00a1f3."B2V_HVP", c00a1f3."timestamp") AS voltage,
                CASE
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '0.000000'::text THEN 'partially-charged'::text
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '1.000000'::text THEN 'fully-charged'::text
                    ELSE 'unknown'::text
                END AS status__battery_pack__full_charge,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '2.000000'::text THEN 'charging'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text THEN 'discharging'::text
                    ELSE 'idle'::text
                END AS status__battery_pack__current_polarity,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text AND last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") < 0::numeric THEN 'regenerative-braking'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text AND last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") > 0::numeric THEN 'driving'::text
                    ELSE 'regeneration-not-applicable'::text
                END AS status__vehicle_control_unit__regenerative_braking
           FROM c00a1f3
          WHERE c00a1f3."timestamp" < now()
          GROUP BY c00a1f3."IMEI"
        ), x AS (
         SELECT
                CASE
                    WHEN position_tracker_advanced.position_tracker_advanced__status = 'position_tracker_advanced__offline'::text AND "vehicle__CAN_frames"."timestamp" < (now() - '00:35:00'::interval) THEN 'disconnected'::text
                    ELSE NULL::text
                END AS status,
            position_tracker_advanced."IMEI",
            position_tracker_advanced.name,
            position_tracker_advanced.depot,
            position_tracker_advanced.city,
            position_tracker_advanced.position_tracker_advanced__others__timestamp,
            position_tracker_advanced.position_tracker_advanced__positions__timestamp,
            position_tracker_advanced.position_tracker_advanced__status,
            position_tracker_advanced."24V",
            position_tracker_advanced."RSSI",
            position_tracker_advanced.ignition,
            position_tracker_advanced."OBD2",
            position_tracker_advanced.sleep_mode,
            position_tracker_advanced.latitude,
            position_tracker_advanced.longitude,
            position_tracker_advanced.speed,
            "vehicle__CAN_frames"."timestamp" AS "vehicle__CAN_frames__timestamp",
            vehicle__1012a1f3."MaxCellV1",
            vehicle__1012a1f3."MinCellV1",
            vehicle__1012a1f3."MaxCellV1" - vehicle__1012a1f3."MinCellV1" AS "DeltaCellV1",
            vehicle__1013a1f3."MaxCellV2",
            vehicle__1013a1f3."MinCellV2",
            vehicle__1013a1f3."MaxCellV2" - vehicle__1013a1f3."MinCellV2" AS "DeltaCellV2",
            vehicle__1014a1f3."MaxCellV3",
            vehicle__1014a1f3."MinCellV3",
            vehicle__1014a1f3."MaxCellV3" - vehicle__1014a1f3."MinCellV3" AS "DeltaCellV3",
            vehicle__1015a1f3."MaxCellV4",
            vehicle__1015a1f3."MinCellV4",
            vehicle__1015a1f3."MaxCellV4" - vehicle__1015a1f3."MinCellV4" AS "DeltaCellV4",
            vehicle__1016a1f3."MaxCellT1",
            vehicle__1016a1f3."MinCellT1",
            vehicle__1017a1f3."MaxCellT2",
            vehicle__1017a1f3."MinCellT2",
            vehicle__1018a1f3."MaxCellT3",
            vehicle__1018a1f3."MinCellT3",
            vehicle__1019a1f3."MaxCellT4",
            vehicle__1019a1f3."MinCellT4",
            vehicle__1820a1f3."SoC",
            vehicle__1821a1f3."SoH",
            vehicle__18fff345.inlet_temperature,
            vehicle__18fff345.outlet_temperature,
            vehicle__c00a1f3.current,
            vehicle__c00a1f3.voltage,
            vehicle__c00a1f3.status__battery_pack__full_charge,
            vehicle__c00a1f3.status__battery_pack__current_polarity,
            vehicle__c00a1f3.status__vehicle_control_unit__regenerative_braking
           FROM "vehicle__CAN_frames"
             RIGHT JOIN position_tracker_advanced ON position_tracker_advanced."IMEI"::bigint = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1820a1f3 ON vehicle__1820a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1821a1f3 ON vehicle__1821a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1012a1f3 ON vehicle__1012a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1013a1f3 ON vehicle__1013a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1014a1f3 ON vehicle__1014a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1015a1f3 ON vehicle__1015a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1016a1f3 ON vehicle__1016a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1017a1f3 ON vehicle__1017a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1018a1f3 ON vehicle__1018a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1019a1f3 ON vehicle__1019a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__18fff345 ON vehicle__18fff345."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__c00a1f3 ON vehicle__c00a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
          WHERE position_tracker_advanced."IMEI" IS NOT NULL
        ), y AS (
         SELECT row_number() OVER (ORDER BY x."IMEI") AS row_number,
            x.status,
            x."IMEI",
            x.name,
            x.depot,
            x.city,
            x.position_tracker_advanced__others__timestamp,
            x.position_tracker_advanced__positions__timestamp,
            x.position_tracker_advanced__status,
            x."24V",
            x."RSSI",
            x.ignition,
            x."OBD2",
            x.sleep_mode,
            x.latitude,
            x.longitude,
            x.speed,
            x."vehicle__CAN_frames__timestamp",
            x."MaxCellV1",
            x."MinCellV1",
            x."DeltaCellV1",
            x."MaxCellV2",
            x."MinCellV2",
            x."DeltaCellV2",
            x."MaxCellV3",
            x."MinCellV3",
            x."DeltaCellV3",
            x."MaxCellV4",
            x."MinCellV4",
            x."DeltaCellV4",
            x."MaxCellT1",
            x."MinCellT1",
            x."MaxCellT2",
            x."MinCellT2",
            x."MaxCellT3",
            x."MinCellT3",
            x."MaxCellT4",
            x."MinCellT4",
            x."SoC",
            x."SoH",
            x.inlet_temperature,
            x.outlet_temperature,
            x.current,
            x.voltage,
            x.status__battery_pack__full_charge,
            x.status__battery_pack__current_polarity,
            x.status__vehicle_control_unit__regenerative_braking
           FROM x
        ), asset AS (
         SELECT y."IMEI"::text AS "IMEI",
            y.name::text AS name,
            y.depot,
            y.city,
            array_remove(ARRAY[y.status, y.status__battery_pack__full_charge, y.status__battery_pack__current_polarity, y.status__vehicle_control_unit__regenerative_braking], NULL::text) AS status,
            y."vehicle__CAN_frames__timestamp" AS timestamp_e,
            y.voltage::double precision AS voltage,
            y.current::double precision AS current,
            y."SoC"::double precision AS "SoC",
            y."SoH"::double precision AS "SoH",
            y."MaxCellV1"::double precision AS "MaxCellV1",
            y."MaxCellV2"::double precision AS "MaxCellV2",
            y."MaxCellV3"::double precision AS "MaxCellV3",
            y."MaxCellV4"::double precision AS "MaxCellV4",
            y."MinCellV1"::double precision AS "MinCellV1",
            y."MinCellV2"::double precision AS "MinCellV2",
            y."MinCellV3"::double precision AS "MinCellV3",
            y."MinCellV4"::double precision AS "MinCellV4",
            y."DeltaCellV1"::double precision AS "DeltaCellV1",
            y."DeltaCellV2"::double precision AS "DeltaCellV2",
            y."DeltaCellV3"::double precision AS "DeltaCellV3",
            y."DeltaCellV4"::double precision AS "DeltaCellV4",
            y."MaxCellT1"::double precision AS "MaxCellT1",
            y."MaxCellT2"::double precision AS "MaxCellT2",
            y."MaxCellT3"::double precision AS "MaxCellT3",
            y."MaxCellT4"::double precision AS "MaxCellT4",
            y."MinCellT1"::double precision AS "MinCellT1",
            y."MinCellT2"::double precision AS "MinCellT2",
            y."MinCellT3"::double precision AS "MinCellT3",
            y."MinCellT4"::double precision AS "MinCellT4",
            y.inlet_temperature::double precision AS inlet_temperature,
            y.outlet_temperature::double precision AS outlet_temperature,
            y.latitude::double precision AS latitude,
            y.longitude::double precision AS longitude,
            y.speed::double precision AS speed,
            y."24V",
            y."RSSI",
            y.ignition,
            y."OBD2"
           FROM y
        ), battery_pack__1010a1f3 AS (
         SELECT "1010a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1010a1f3"."timestamp", "1010a1f3"."timestamp")) AS "timestamp",
                CASE
                    WHEN last(concat("1010a1f3"."B2V_CellVoltTooHigh2", "1010a1f3"."B2V_CellVoltTooHigh3", "1010a1f3"."B2V_CellVoltTooHigh4", "1010a1f3"."B2V_CellVoltTooLow2", "1010a1f3"."B2V_CellVoltTooLow3", "1010a1f3"."B2V_CellVoltTooLow4", "1010a1f3"."B2V_BatVoltTooHigh3", "1010a1f3"."B2V_BatVoltTooLow3", "1010a1f3"."B2V_BatTempTooHigh2", "1010a1f3"."B2V_BatTempTooHigh3", "1010a1f3"."B2V_BatTempTooHigh4", "1010a1f3"."B2V_BatTempTooLow1", "1010a1f3"."B2V_DchgOverI2", "1010a1f3"."B2V_FeedbackChrgOverI2", "1010a1f3"."B2V_IntranetCANComm3", "1010a1f3"."B2V_SysConnFault4"), "1010a1f3"."timestamp") = '0.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.000000'::text THEN 'No Fault'::text
                    ELSE 'Fault'::text
                END AS fault__1010a1f3
           FROM "1010a1f3"
          GROUP BY "1010a1f3"."IMEI"
        ), battery_pack__1011a1f3 AS (
         SELECT "1011a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1011a1f3"."timestamp", "1011a1f3"."timestamp")) AS "timestamp",
                CASE
                    WHEN last(concat("1011a1f3"."B2V_CellVoltDiff1", "1011a1f3"."B2V_BMSWorkVoltError3", "1011a1f3"."B2V_TempDiff1", "1011a1f3"."B2V_TempNotControl4", "1011a1f3"."B2V_SOCTooLow1", "1011a1f3"."B2V_SOCTooLow2", "1011a1f3"."B2V_SOCTooHigh1", "1011a1f3"."B2V_SOCJump1", "1011a1f3"."B2V_PowerCANCom3", "1011a1f3"."B2V_MiddleCANCom3", "1011a1f3"."B2V_TMSCom1", "1011a1f3"."B2V_TMSFault2", "1011a1f3"."B2V_IncorrectConfigStr4", "1011a1f3"."B2V_StrVDiffTooLarge3", "1011a1f3"."B2V_ChrgSignalLose3", "1011a1f3"."B2V_HVILFault3", "1011a1f3"."B2V_CellVInRangeOutFault2", "1011a1f3"."B2V_CellStandTDetectFault1", "1011a1f3"."B2V_CellStandTDetectFault2", "1011a1f3"."B2V_LECUBoardTTooHigh1", "1011a1f3"."B2V_LECUVChipTTooHigh1"), "1011a1f3"."timestamp") = '0.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.000000'::text THEN 'No Fault'::text
                    ELSE 'Fault'::text
                END AS fault__1011a1f3
           FROM "1011a1f3"
          GROUP BY "1011a1f3"."IMEI"
        ), bus_faults_renamed AS (
         SELECT battery_pack__1010a1f3."IMEI"::text AS imei,
            lower(battery_pack__1010a1f3.fault__1010a1f3) AS fault1,
            lower(battery_pack__1011a1f3.fault__1011a1f3) AS fault2
           FROM battery_pack__1010a1f3
             JOIN battery_pack__1011a1f3 ON battery_pack__1010a1f3."IMEI" = battery_pack__1011a1f3."IMEI"
          ORDER BY battery_pack__1010a1f3."IMEI"
        ), bus_faults_data_cte AS (
         SELECT bus_faults_renamed.imei,
                CASE
                    WHEN bus_faults_renamed.fault1 = 'fault'::text OR bus_faults_renamed.fault2 = 'fault'::text THEN 'in-fault'::text
                    ELSE NULL::text
                END AS has_fault
           FROM bus_faults_renamed
        ), bus_battery_data_renamed AS (
         SELECT asset."IMEI" AS imei,
            ( SELECT array_agg(lower(btrim(status.status))) AS status
                   FROM unnest(asset.status) status(status)) AS status,
            btrim(replace(asset.name, ' '::text, ''::text)) AS bus_number,
            btrim(lower(asset.depot)) AS depot,
            btrim(lower(asset.city)) AS city,
            asset.latitude,
            asset.longitude,
            asset."SoC" AS soc,
            asset."SoH" AS soh,
            asset.current,
            asset.voltage,
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
            asset.speed,
            asset.timestamp_e AS "timestamp",
                CASE
                    WHEN asset."24V" IS NULL THEN 'off'::text
                    WHEN asset."24V"::numeric > 24::numeric THEN 'on'::text
                    ELSE 'off'::text
                END AS external_power_status,
            asset."RSSI"::numeric AS signal_strength,
                CASE
                    WHEN asset."OBD2" IS NULL THEN 'off'::text
                    WHEN asset."OBD2" = true THEN 'on'::text
                    ELSE 'off'::text
                END AS can_data_status,
                CASE
                    WHEN asset.speed IS NULL THEN 'off'::text
                    WHEN asset.speed::integer <= 0 THEN 'off'::text
                    ELSE 'on'::text
                END AS bus_running_status,
                CASE
                    WHEN ('regenerative-braking'::text IN ( SELECT lower(unnest(asset.status)) AS lower)) THEN 'on'::text
                    ELSE 'off'::text
                END AS regeneration_status,
                CASE
                    WHEN ('online'::text IN ( SELECT lower(unnest(asset.status)) AS lower)) THEN 'on'::text
                    ELSE 'off'::text
                END AS bus_status,
                CASE
                    WHEN 'charging'::text = ANY (asset.status) THEN 'charging'::text
                    WHEN 'discharging'::text = ANY (asset.status) THEN 'discharging'::text
                    WHEN 'idle'::text = ANY (asset.status) THEN 'idle'::text
                    ELSE NULL::text
                END AS bms_status,
            fd.has_fault
           FROM asset
             LEFT JOIN bus_faults_data_cte fd ON asset."IMEI" = fd.imei
        ), bus_battery_data_cte AS (
         SELECT bus_battery_data_renamed.imei,
            array_remove((bus_battery_data_renamed.status || ARRAY[bus_battery_data_renamed.has_fault]) || ARRAY[
                CASE
                    WHEN bus_battery_data_renamed.can_data_status = 'off'::text THEN 'in-field'::text
                    ELSE NULL::text
                END], NULL::text) AS status,
            bus_battery_data_renamed.bus_number,
            bus_battery_data_renamed.depot,
            bus_battery_data_renamed.city,
            bus_battery_data_renamed.latitude,
            bus_battery_data_renamed.longitude,
            COALESCE(bus_battery_data_renamed.soc, 0::double precision) AS soc,
            COALESCE(bus_battery_data_renamed.soh, 0::double precision) AS soh,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 0::numeric::double precision
                    ELSE COALESCE(bus_battery_data_renamed.current, 0::double precision)
                END AS current,
            COALESCE(bus_battery_data_renamed.voltage, 0::double precision) AS voltage,
            COALESCE(bus_battery_data_renamed.inlet_temperature, 0::double precision) AS inlet_temperature,
            COALESCE(bus_battery_data_renamed.outlet_temperature, 0::double precision) AS outlet_temperature,
            COALESCE(bus_battery_data_renamed.max_cell_v1, 0::double precision) AS max_cell_v1,
            COALESCE(bus_battery_data_renamed.max_cell_v2, 0::double precision) AS max_cell_v2,
            COALESCE(bus_battery_data_renamed.max_cell_v3, 0::double precision) AS max_cell_v3,
            COALESCE(bus_battery_data_renamed.max_cell_v4, 0::double precision) AS max_cell_v4,
            COALESCE(bus_battery_data_renamed.min_cell_v1, 0::double precision) AS min_cell_v1,
            COALESCE(bus_battery_data_renamed.min_cell_v2, 0::double precision) AS min_cell_v2,
            COALESCE(bus_battery_data_renamed.min_cell_v3, 0::double precision) AS min_cell_v3,
            COALESCE(bus_battery_data_renamed.min_cell_v4, 0::double precision) AS min_cell_v4,
            COALESCE(bus_battery_data_renamed.delta_cell_v1, 0::double precision) AS delta_cell_v1,
            COALESCE(bus_battery_data_renamed.delta_cell_v2, 0::double precision) AS delta_cell_v2,
            COALESCE(bus_battery_data_renamed.delta_cell_v3, 0::double precision) AS delta_cell_v3,
            COALESCE(bus_battery_data_renamed.delta_cell_v4, 0::double precision) AS delta_cell_v4,
            COALESCE(bus_battery_data_renamed.max_cell_t1, 0::double precision) AS max_cell_t1,
            COALESCE(bus_battery_data_renamed.max_cell_t2, 0::double precision) AS max_cell_t2,
            COALESCE(bus_battery_data_renamed.max_cell_t3, 0::double precision) AS max_cell_t3,
            COALESCE(bus_battery_data_renamed.max_cell_t4, 0::double precision) AS max_cell_t4,
            COALESCE(bus_battery_data_renamed.min_cell_t1, 0::double precision) AS min_cell_t1,
            COALESCE(bus_battery_data_renamed.min_cell_t2, 0::double precision) AS min_cell_t2,
            COALESCE(bus_battery_data_renamed.min_cell_t3, 0::double precision) AS min_cell_t3,
            COALESCE(bus_battery_data_renamed.min_cell_t4, 0::double precision) AS min_cell_t4,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 0::double precision
                    ELSE bus_battery_data_renamed.speed
                END AS speed,
            bus_battery_data_renamed."timestamp",
            COALESCE(bus_battery_data_renamed.external_power_status, 'off'::text) AS external_power_status,
            COALESCE(bus_battery_data_renamed.signal_strength, 1::numeric) AS signal_strength,
            COALESCE(bus_battery_data_renamed.can_data_status, 'off'::text) AS can_data_status,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text OR bus_battery_data_renamed.speed = 0::double precision THEN 'off'::text
                    ELSE COALESCE(bus_battery_data_renamed.bus_running_status, 'off'::text)
                END AS bus_running_status,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 'off'::text
                    ELSE COALESCE(bus_battery_data_renamed.regeneration_status, 'off'::text)
                END AS regeneration_status,
            COALESCE(bus_battery_data_renamed.bus_status, 'off'::text) AS bus_status,
            COALESCE(bus_battery_data_renamed.bms_status, 'idle'::text) AS bms_status
           FROM bus_battery_data_renamed
          WHERE NOT (bus_battery_data_renamed.imei IS NULL OR bus_battery_data_renamed.bus_number IS NULL OR bus_battery_data_renamed.depot IS NULL OR bus_battery_data_renamed.city IS NULL OR bus_battery_data_renamed.latitude IS NULL OR bus_battery_data_renamed.longitude IS NULL OR bus_battery_data_renamed.soc IS NULL OR bus_battery_data_renamed.soh IS NULL OR bus_battery_data_renamed.current IS NULL OR bus_battery_data_renamed.voltage IS NULL OR bus_battery_data_renamed.inlet_temperature IS NULL OR bus_battery_data_renamed.outlet_temperature IS NULL OR bus_battery_data_renamed.max_cell_v1 IS NULL OR bus_battery_data_renamed.max_cell_v2 IS NULL OR bus_battery_data_renamed.max_cell_v3 IS NULL OR bus_battery_data_renamed.max_cell_v4 IS NULL OR bus_battery_data_renamed.min_cell_v1 IS NULL OR bus_battery_data_renamed.min_cell_v2 IS NULL OR bus_battery_data_renamed.min_cell_v3 IS NULL OR bus_battery_data_renamed.min_cell_v4 IS NULL OR bus_battery_data_renamed.delta_cell_v1 IS NULL OR bus_battery_data_renamed.delta_cell_v2 IS NULL OR bus_battery_data_renamed.delta_cell_v3 IS NULL OR bus_battery_data_renamed.delta_cell_v4 IS NULL OR bus_battery_data_renamed.max_cell_t1 IS NULL OR bus_battery_data_renamed.max_cell_t2 IS NULL OR bus_battery_data_renamed.max_cell_t3 IS NULL OR bus_battery_data_renamed.max_cell_t4 IS NULL OR bus_battery_data_renamed.min_cell_t1 IS NULL OR bus_battery_data_renamed.min_cell_t2 IS NULL OR bus_battery_data_renamed.min_cell_t3 IS NULL OR bus_battery_data_renamed.min_cell_t4 IS NULL OR bus_battery_data_renamed.speed IS NULL OR bus_battery_data_renamed."timestamp" IS NULL OR bus_battery_data_renamed.external_power_status IS NULL OR bus_battery_data_renamed.signal_strength IS NULL OR bus_battery_data_renamed.can_data_status IS NULL OR bus_battery_data_renamed.bus_running_status IS NULL OR bus_battery_data_renamed.regeneration_status IS NULL OR bus_battery_data_renamed.bus_status IS NULL OR bus_battery_data_renamed.bms_status IS NULL OR bus_battery_data_renamed.longitude::numeric <= 0::numeric OR bus_battery_data_renamed.latitude::numeric <= 0::numeric)
        )
 SELECT bus_battery_data_cte.imei,
    bus_battery_data_cte.status,
    bus_battery_data_cte.bus_number,
    bus_battery_data_cte.depot,
    bus_battery_data_cte.city,
    bus_battery_data_cte.latitude,
    bus_battery_data_cte.longitude,
    bus_battery_data_cte.soc,
    bus_battery_data_cte.soh,
    bus_battery_data_cte.current,
    bus_battery_data_cte.voltage,
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
    bus_battery_data_cte.speed,
    bus_battery_data_cte."timestamp",
    bus_battery_data_cte.external_power_status,
    bus_battery_data_cte.signal_strength,
    bus_battery_data_cte.can_data_status,
    bus_battery_data_cte.bus_running_status,
    bus_battery_data_cte.regeneration_status,
    bus_battery_data_cte.bus_status,
    bus_battery_data_cte.bms_status
   FROM bus_battery_data_cte;

-------------------------------------------------------------------
-- modified ::

 WITH position_tracker_advanced__others AS (
         SELECT others."IMEI",
            time_bucket('00:00:01'::interval, last(others."createdAt", others."createdAt")) AS "timestamp",
            last(others."MainBat", others.iat) AS "24V",
            last(others."SIG_QUAL", others.iat) AS "RSSI",
            last(others."IGN_STATE", others.iat) AS ignition,
            last(others."CAN_STATE", others.iat) AS "OBD2",
            last(others.sleep_mode, others.iat) AS sleep_mode
           FROM others
          GROUP BY others."IMEI"
        ), position_tracker_advanced__positions AS (
         SELECT tc_positions.deviceid,
            last(tc_positions.fixtime, tc_positions.fixtime) + '05:30:00'::interval AS "timestamp",
            last(tc_positions.latitude, tc_positions.fixtime)::text AS latitude,
            last(tc_positions.longitude, tc_positions.fixtime)::text AS longitude,
            last(tc_positions.speed, tc_positions.fixtime)::text AS speed
           FROM traccar.tc_positions
          WHERE tc_positions.latitude <> 0::double precision AND tc_positions.longitude <> 0::double precision
          GROUP BY tc_positions.deviceid
        ), position_tracker_advanced__devices AS (
         SELECT tc_devices.uniqueid AS "IMEI",
            tc_devices.name,
            initcap(tc_devices.attributes::jsonb ->> 'depot'::text) AS depot,
            initcap(tc_devices.attributes::jsonb ->> 'city'::text) AS city,
            tc_devices.id
           FROM traccar.tc_devices
          WHERE tc_devices.uniqueid::text <> tc_devices.name::text
        ), position_tracker_advanced AS (
         SELECT position_tracker_advanced__devices."IMEI",
            position_tracker_advanced__devices.name,
            position_tracker_advanced__devices.depot,
            position_tracker_advanced__devices.city,
            position_tracker_advanced__others."timestamp" AS position_tracker_advanced__others__timestamp,
            position_tracker_advanced__positions."timestamp" AS position_tracker_advanced__positions__timestamp,
                CASE
                    WHEN position_tracker_advanced__others."timestamp" < (now() - '00:35:00'::interval) AND position_tracker_advanced__positions."timestamp" < (now() - '00:35:00'::interval) THEN 'position_tracker_advanced__offline'::text
                    ELSE 'position_tracker_advanced__online'::text
                END AS position_tracker_advanced__status,
            position_tracker_advanced__others."24V",
            position_tracker_advanced__others."RSSI",
            position_tracker_advanced__others.ignition,
            position_tracker_advanced__others."OBD2",
            position_tracker_advanced__others.sleep_mode,
            position_tracker_advanced__positions.latitude,
            position_tracker_advanced__positions.longitude,
            position_tracker_advanced__positions.speed
           FROM position_tracker_advanced__devices
             LEFT JOIN position_tracker_advanced__positions ON position_tracker_advanced__positions.deviceid = position_tracker_advanced__devices.id
             LEFT JOIN position_tracker_advanced__others ON position_tracker_advanced__others."IMEI" = position_tracker_advanced__devices."IMEI"::bigint
        ), "vehicle__CAN_frames" AS (
         SELECT "CAN_frames"."IMEI",
            time_bucket('00:00:01'::interval, last("CAN_frames"."timestamp", "CAN_frames"."timestamp")) AS "timestamp"
           FROM "CAN_frames"
          WHERE "CAN_frames"."timestamp" < now()
          GROUP BY "CAN_frames"."IMEI"
        ), vehicle__18fff345 AS (
         SELECT "18fff345"."IMEI",
            last("18fff345"."T2B_TIn", "18fff345"."timestamp") AS inlet_temperature,
            last("18fff345"."T2B_TOut", "18fff345"."timestamp") AS outlet_temperature
           FROM "18fff345"
          WHERE "18fff345"."timestamp" < now()
          GROUP BY "18fff345"."IMEI"
        ), vehicle__1012a1f3 AS (
         SELECT "1012a1f3"."IMEI",
            last("1012a1f3"."B2V_MaxCellV1", "1012a1f3"."timestamp") AS "MaxCellV1",
            last("1012a1f3"."B2V_MinCellV1", "1012a1f3"."timestamp") AS "MinCellV1"
           FROM "1012a1f3"
          WHERE "1012a1f3"."timestamp" < now()
          GROUP BY "1012a1f3"."IMEI"
        ), vehicle__1013a1f3 AS (
         SELECT "1013a1f3"."IMEI",
            last("1013a1f3"."B2V_MaxCellV2", "1013a1f3"."timestamp") AS "MaxCellV2",
            last("1013a1f3"."B2V_MinCellV2", "1013a1f3"."timestamp") AS "MinCellV2"
           FROM "1013a1f3"
          WHERE "1013a1f3"."timestamp" < now()
          GROUP BY "1013a1f3"."IMEI"
        ), vehicle__1014a1f3 AS (
         SELECT "1014a1f3"."IMEI",
            last("1014a1f3"."B2V_MaxCellV3", "1014a1f3"."timestamp") AS "MaxCellV3",
            last("1014a1f3"."B2V_MinCellV3", "1014a1f3"."timestamp") AS "MinCellV3"
           FROM "1014a1f3"
          WHERE "1014a1f3"."timestamp" < now()
          GROUP BY "1014a1f3"."IMEI"
        ), vehicle__1015a1f3 AS (
         SELECT "1015a1f3"."IMEI",
            last("1015a1f3"."B2V_MaxCellV4", "1015a1f3"."timestamp") AS "MaxCellV4",
            last("1015a1f3"."B2V_MinCellV4", "1015a1f3"."timestamp") AS "MinCellV4"
           FROM "1015a1f3"
          WHERE "1015a1f3"."timestamp" < now()
          GROUP BY "1015a1f3"."IMEI"
        ), vehicle__1016a1f3 AS (
         SELECT "1016a1f3"."IMEI",
            last("1016a1f3"."B2V_MaxCellT1", "1016a1f3"."timestamp") AS "MaxCellT1",
            last("1016a1f3"."B2V_MinCellT1", "1016a1f3"."timestamp") AS "MinCellT1"
           FROM "1016a1f3"
          WHERE "1016a1f3"."timestamp" < now()
          GROUP BY "1016a1f3"."IMEI"
        ), vehicle__1017a1f3 AS (
         SELECT "1017a1f3"."IMEI",
            last("1017a1f3"."B2V_MaxCellT2", "1017a1f3"."timestamp") AS "MaxCellT2",
            last("1017a1f3"."B2V_MinCellT2", "1017a1f3"."timestamp") AS "MinCellT2"
           FROM "1017a1f3"
          WHERE "1017a1f3"."timestamp" < now()
          GROUP BY "1017a1f3"."IMEI"
        ), vehicle__1018a1f3 AS (
         SELECT "1018a1f3"."IMEI",
            last("1018a1f3"."B2V_MaxCellT3", "1018a1f3"."timestamp") AS "MaxCellT3",
            last("1018a1f3"."B2V_MinCellT3", "1018a1f3"."timestamp") AS "MinCellT3"
           FROM "1018a1f3"
          WHERE "1018a1f3"."timestamp" < now()
          GROUP BY "1018a1f3"."IMEI"
        ), vehicle__1019a1f3 AS (
         SELECT "1019a1f3"."IMEI",
            last("1019a1f3"."B2V_MaxCellT4", "1019a1f3"."timestamp") AS "MaxCellT4",
            last("1019a1f3"."B2V_MinCellT4", "1019a1f3"."timestamp") AS "MinCellT4"
           FROM "1019a1f3"
          WHERE "1019a1f3"."timestamp" < now()
          GROUP BY "1019a1f3"."IMEI"
        ), vehicle__1820a1f3 AS (
         SELECT "1820a1f3"."IMEI",
            last("1820a1f3"."B2V_SOC", "1820a1f3"."timestamp") AS "SoC"
           FROM "1820a1f3"
          WHERE "1820a1f3"."timestamp" < now()
          GROUP BY "1820a1f3"."IMEI"
        ), vehicle__1821a1f3 AS (
         SELECT "1821a1f3"."IMEI",
            last("1821a1f3"."B2V_SOH", "1821a1f3"."timestamp") AS "SoH"
           FROM "1821a1f3"
          WHERE "1821a1f3"."timestamp" < now()
          GROUP BY "1821a1f3"."IMEI"
        ), vehicle__c00a1f3 AS (
         SELECT c00a1f3."IMEI",
            last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") AS current,
            last(c00a1f3."B2V_HVP", c00a1f3."timestamp") AS voltage,
                CASE
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '0.000000'::text THEN 'partially-charged'::text
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '1.000000'::text THEN 'fully-charged'::text
                    ELSE 'unknown'::text
                END AS status__battery_pack__full_charge,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '2.000000'::text THEN 'charging'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text THEN 'discharging'::text
                    ELSE 'idle'::text
                END AS status__battery_pack__current_polarity,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text AND last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") < 0::numeric THEN 'regenerative-braking'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text AND last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") > 0::numeric THEN 'driving'::text
                    ELSE 'regeneration-not-applicable'::text
                END AS status__vehicle_control_unit__regenerative_braking
           FROM c00a1f3
          WHERE c00a1f3."timestamp" < now()
          GROUP BY c00a1f3."IMEI"
        ), x AS (
         SELECT
                CASE
                    WHEN position_tracker_advanced.position_tracker_advanced__status = 'position_tracker_advanced__offline'::text AND "vehicle__CAN_frames"."timestamp" < (now() - '00:35:00'::interval) THEN 'disconnected'::text
                    ELSE NULL::text
                END AS status,
            position_tracker_advanced."IMEI",
            position_tracker_advanced.name,
            position_tracker_advanced.depot,
            position_tracker_advanced.city,
            position_tracker_advanced.position_tracker_advanced__others__timestamp,
            position_tracker_advanced.position_tracker_advanced__positions__timestamp,
            position_tracker_advanced.position_tracker_advanced__status,
            position_tracker_advanced."24V",
            position_tracker_advanced."RSSI",
            position_tracker_advanced.ignition,
            position_tracker_advanced."OBD2",
            position_tracker_advanced.sleep_mode,
            position_tracker_advanced.latitude,
            position_tracker_advanced.longitude,
            position_tracker_advanced.speed,
            "vehicle__CAN_frames"."timestamp" AS "vehicle__CAN_frames__timestamp",
            vehicle__1012a1f3."MaxCellV1",
            vehicle__1012a1f3."MinCellV1",
            vehicle__1012a1f3."MaxCellV1" - vehicle__1012a1f3."MinCellV1" AS "DeltaCellV1",
            vehicle__1013a1f3."MaxCellV2",
            vehicle__1013a1f3."MinCellV2",
            vehicle__1013a1f3."MaxCellV2" - vehicle__1013a1f3."MinCellV2" AS "DeltaCellV2",
            vehicle__1014a1f3."MaxCellV3",
            vehicle__1014a1f3."MinCellV3",
            vehicle__1014a1f3."MaxCellV3" - vehicle__1014a1f3."MinCellV3" AS "DeltaCellV3",
            vehicle__1015a1f3."MaxCellV4",
            vehicle__1015a1f3."MinCellV4",
            vehicle__1015a1f3."MaxCellV4" - vehicle__1015a1f3."MinCellV4" AS "DeltaCellV4",
            vehicle__1016a1f3."MaxCellT1",
            vehicle__1016a1f3."MinCellT1",
            vehicle__1017a1f3."MaxCellT2",
            vehicle__1017a1f3."MinCellT2",
            vehicle__1018a1f3."MaxCellT3",
            vehicle__1018a1f3."MinCellT3",
            vehicle__1019a1f3."MaxCellT4",
            vehicle__1019a1f3."MinCellT4",
            vehicle__1820a1f3."SoC",
            vehicle__1821a1f3."SoH",
            vehicle__18fff345.inlet_temperature,
            vehicle__18fff345.outlet_temperature,
            vehicle__c00a1f3.current,
            vehicle__c00a1f3.voltage,
            vehicle__c00a1f3.status__battery_pack__full_charge,
            vehicle__c00a1f3.status__battery_pack__current_polarity,
            vehicle__c00a1f3.status__vehicle_control_unit__regenerative_braking
           FROM "vehicle__CAN_frames"
             RIGHT JOIN position_tracker_advanced ON position_tracker_advanced."IMEI"::bigint = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1820a1f3 ON vehicle__1820a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1821a1f3 ON vehicle__1821a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1012a1f3 ON vehicle__1012a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1013a1f3 ON vehicle__1013a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1014a1f3 ON vehicle__1014a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1015a1f3 ON vehicle__1015a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1016a1f3 ON vehicle__1016a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1017a1f3 ON vehicle__1017a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1018a1f3 ON vehicle__1018a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__1019a1f3 ON vehicle__1019a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__18fff345 ON vehicle__18fff345."IMEI" = "vehicle__CAN_frames"."IMEI"
             FULL JOIN vehicle__c00a1f3 ON vehicle__c00a1f3."IMEI" = "vehicle__CAN_frames"."IMEI"
          WHERE position_tracker_advanced."IMEI" IS NOT NULL
        ), y AS (
         SELECT row_number() OVER (ORDER BY x."IMEI") AS row_number,
            x.status,
            x."IMEI",
            x.name,
            x.depot,
            x.city,
            x.position_tracker_advanced__others__timestamp,
            x.position_tracker_advanced__positions__timestamp,
            x.position_tracker_advanced__status,
            x."24V",
            x."RSSI",
            x.ignition,
            x."OBD2",
            x.sleep_mode,
            x.latitude,
            x.longitude,
            x.speed,
            x."vehicle__CAN_frames__timestamp",
            x."MaxCellV1",
            x."MinCellV1",
            x."DeltaCellV1",
            x."MaxCellV2",
            x."MinCellV2",
            x."DeltaCellV2",
            x."MaxCellV3",
            x."MinCellV3",
            x."DeltaCellV3",
            x."MaxCellV4",
            x."MinCellV4",
            x."DeltaCellV4",
            x."MaxCellT1",
            x."MinCellT1",
            x."MaxCellT2",
            x."MinCellT2",
            x."MaxCellT3",
            x."MinCellT3",
            x."MaxCellT4",
            x."MinCellT4",
            x."SoC",
            x."SoH",
            x.inlet_temperature,
            x.outlet_temperature,
            x.current,
            x.voltage,
            x.status__battery_pack__full_charge,
            x.status__battery_pack__current_polarity,
            x.status__vehicle_control_unit__regenerative_braking
           FROM x
        ), asset AS (
         SELECT y."IMEI"::text AS "IMEI",
            y.name::text AS name,
            y.depot,
            y.city,
            array_remove(ARRAY[y.status, y.status__battery_pack__full_charge, y.status__battery_pack__current_polarity, y.status__vehicle_control_unit__regenerative_braking], NULL::text) AS status,
            y."vehicle__CAN_frames__timestamp" AS timestamp_e,
            y.voltage::double precision AS voltage,
            y.current::double precision AS current,
            y."SoC"::double precision AS "SoC",
            y."SoH"::double precision AS "SoH",
            y."MaxCellV1"::double precision AS "MaxCellV1",
            y."MaxCellV2"::double precision AS "MaxCellV2",
            y."MaxCellV3"::double precision AS "MaxCellV3",
            y."MaxCellV4"::double precision AS "MaxCellV4",
            y."MinCellV1"::double precision AS "MinCellV1",
            y."MinCellV2"::double precision AS "MinCellV2",
            y."MinCellV3"::double precision AS "MinCellV3",
            y."MinCellV4"::double precision AS "MinCellV4",
            y."DeltaCellV1"::double precision AS "DeltaCellV1",
            y."DeltaCellV2"::double precision AS "DeltaCellV2",
            y."DeltaCellV3"::double precision AS "DeltaCellV3",
            y."DeltaCellV4"::double precision AS "DeltaCellV4",
            y."MaxCellT1"::double precision AS "MaxCellT1",
            y."MaxCellT2"::double precision AS "MaxCellT2",
            y."MaxCellT3"::double precision AS "MaxCellT3",
            y."MaxCellT4"::double precision AS "MaxCellT4",
            y."MinCellT1"::double precision AS "MinCellT1",
            y."MinCellT2"::double precision AS "MinCellT2",
            y."MinCellT3"::double precision AS "MinCellT3",
            y."MinCellT4"::double precision AS "MinCellT4",
            y.inlet_temperature::double precision AS inlet_temperature,
            y.outlet_temperature::double precision AS outlet_temperature,
            y.latitude::double precision AS latitude,
            y.longitude::double precision AS longitude,
            y.speed::double precision AS speed,
            y."24V",
            y."RSSI",
            y.ignition,
            y."OBD2"
           FROM y
        ), battery_pack__1010a1f3 AS (
         SELECT "1010a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1010a1f3"."timestamp", "1010a1f3"."timestamp")) AS "timestamp",
                CASE
                    WHEN last(concat("1010a1f3"."B2V_CellVoltTooHigh2", "1010a1f3"."B2V_CellVoltTooHigh3", "1010a1f3"."B2V_CellVoltTooHigh4", "1010a1f3"."B2V_CellVoltTooLow2", "1010a1f3"."B2V_CellVoltTooLow3", "1010a1f3"."B2V_CellVoltTooLow4", "1010a1f3"."B2V_BatVoltTooHigh3", "1010a1f3"."B2V_BatVoltTooLow3", "1010a1f3"."B2V_BatTempTooHigh2", "1010a1f3"."B2V_BatTempTooHigh3", "1010a1f3"."B2V_BatTempTooHigh4", "1010a1f3"."B2V_BatTempTooLow1", "1010a1f3"."B2V_DchgOverI2", "1010a1f3"."B2V_FeedbackChrgOverI2", "1010a1f3"."B2V_IntranetCANComm3", "1010a1f3"."B2V_SysConnFault4"), "1010a1f3"."timestamp") = '0.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.000000'::text THEN 'No Fault'::text
                    ELSE 'Fault'::text
                END AS fault__1010a1f3
           FROM "1010a1f3"
          GROUP BY "1010a1f3"."IMEI"
        ), battery_pack__1011a1f3 AS (
         SELECT "1011a1f3"."IMEI",
            time_bucket('00:00:01'::interval, last("1011a1f3"."timestamp", "1011a1f3"."timestamp")) AS "timestamp",
                CASE
                    WHEN last(concat("1011a1f3"."B2V_CellVoltDiff1", "1011a1f3"."B2V_BMSWorkVoltError3", "1011a1f3"."B2V_TempDiff1", "1011a1f3"."B2V_TempNotControl4", "1011a1f3"."B2V_SOCTooLow1", "1011a1f3"."B2V_SOCTooLow2", "1011a1f3"."B2V_SOCTooHigh1", "1011a1f3"."B2V_SOCJump1", "1011a1f3"."B2V_PowerCANCom3", "1011a1f3"."B2V_MiddleCANCom3", "1011a1f3"."B2V_TMSCom1", "1011a1f3"."B2V_TMSFault2", "1011a1f3"."B2V_IncorrectConfigStr4", "1011a1f3"."B2V_StrVDiffTooLarge3", "1011a1f3"."B2V_ChrgSignalLose3", "1011a1f3"."B2V_HVILFault3", "1011a1f3"."B2V_CellVInRangeOutFault2", "1011a1f3"."B2V_CellStandTDetectFault1", "1011a1f3"."B2V_CellStandTDetectFault2", "1011a1f3"."B2V_LECUBoardTTooHigh1", "1011a1f3"."B2V_LECUVChipTTooHigh1"), "1011a1f3"."timestamp") = '0.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.0000000.000000'::text THEN 'No Fault'::text
                    ELSE 'Fault'::text
                END AS fault__1011a1f3
           FROM "1011a1f3"
          GROUP BY "1011a1f3"."IMEI"
        ), bus_faults_renamed AS (
         SELECT battery_pack__1010a1f3."IMEI"::text AS imei,
            lower(battery_pack__1010a1f3.fault__1010a1f3) AS fault1,
            lower(battery_pack__1011a1f3.fault__1011a1f3) AS fault2
           FROM battery_pack__1010a1f3
             JOIN battery_pack__1011a1f3 ON battery_pack__1010a1f3."IMEI" = battery_pack__1011a1f3."IMEI"
          ORDER BY battery_pack__1010a1f3."IMEI"
        ), bus_faults_data_cte AS (
         SELECT bus_faults_renamed.imei,
                CASE
                    WHEN bus_faults_renamed.fault1 = 'fault'::text OR bus_faults_renamed.fault2 = 'fault'::text THEN 'in-fault'::text
                    ELSE NULL::text
                END AS has_fault
           FROM bus_faults_renamed
        ), bus_battery_data_renamed AS (
         SELECT asset."IMEI" AS imei,
            ( SELECT array_agg(lower(btrim(status.status))) AS status
                   FROM unnest(asset.status) status(status)) AS status,
            btrim(replace(asset.name, ' '::text, ''::text)) AS bus_number,
            btrim(lower(asset.depot)) AS depot,
            btrim(lower(asset.city)) AS city,
            asset.latitude,
            asset.longitude,
            asset."SoC" AS soc,
            asset."SoH" AS soh,
            asset.current,
            asset.voltage,
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
            asset.speed,
            asset.timestamp_e AS "timestamp",
                CASE
                    WHEN asset."24V" IS NULL THEN 'off'::text
                    WHEN asset."24V"::numeric > 24::numeric THEN 'on'::text
                    ELSE 'off'::text
                END AS external_power_status,
            asset."RSSI"::numeric AS signal_strength,
                CASE
                    WHEN asset."OBD2" IS NULL THEN 'off'::text
                    WHEN asset."OBD2" = true THEN 'on'::text
                    ELSE 'off'::text
                END AS can_data_status,
                CASE
                    WHEN asset.speed IS NULL THEN 'off'::text
                    WHEN asset.speed::integer <= 0 THEN 'off'::text
                    ELSE 'on'::text
                END AS bus_running_status,
                CASE
                    WHEN ('regenerative-braking'::text IN ( SELECT lower(unnest(asset.status)) AS lower)) THEN 'on'::text
                    ELSE 'off'::text
                END AS regeneration_status,
                CASE
                    WHEN ('online'::text IN ( SELECT lower(unnest(asset.status)) AS lower)) THEN 'on'::text
                    ELSE 'off'::text
                END AS bus_status,
                CASE
                    WHEN 'charging'::text = ANY (asset.status) THEN 'charging'::text
                    WHEN 'discharging'::text = ANY (asset.status) THEN 'discharging'::text
                    WHEN 'idle'::text = ANY (asset.status) THEN 'idle'::text
                    ELSE NULL::text
                END AS bms_status,
            fd.has_fault
           FROM asset
             LEFT JOIN bus_faults_data_cte fd ON asset."IMEI" = fd.imei
        ), bus_battery_data_cte AS (
         SELECT bus_battery_data_renamed.imei,
            array_remove((bus_battery_data_renamed.status || ARRAY[bus_battery_data_renamed.has_fault]) || ARRAY[
                CASE
                    WHEN bus_battery_data_renamed.can_data_status = 'off'::text THEN 'in-field'::text
                    ELSE NULL::text
                END], NULL::text) AS status,
            bus_battery_data_renamed.bus_number,
            bus_battery_data_renamed.depot,
            bus_battery_data_renamed.city,
            bus_battery_data_renamed.latitude,
            bus_battery_data_renamed.longitude,
            COALESCE(bus_battery_data_renamed.soc, 0::double precision) AS soc,
            COALESCE(bus_battery_data_renamed.soh, 0::double precision) AS soh,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 0::numeric::double precision
                    ELSE COALESCE(bus_battery_data_renamed.current, 0::double precision)
                END AS current,
            COALESCE(bus_battery_data_renamed.voltage, 0::double precision) AS voltage,
            COALESCE(bus_battery_data_renamed.inlet_temperature, 0::double precision) AS inlet_temperature,
            COALESCE(bus_battery_data_renamed.outlet_temperature, 0::double precision) AS outlet_temperature,
            COALESCE(bus_battery_data_renamed.max_cell_v1, 0::double precision) AS max_cell_v1,
            COALESCE(bus_battery_data_renamed.max_cell_v2, 0::double precision) AS max_cell_v2,
            COALESCE(bus_battery_data_renamed.max_cell_v3, 0::double precision) AS max_cell_v3,
            COALESCE(bus_battery_data_renamed.max_cell_v4, 0::double precision) AS max_cell_v4,
            COALESCE(bus_battery_data_renamed.min_cell_v1, 0::double precision) AS min_cell_v1,
            COALESCE(bus_battery_data_renamed.min_cell_v2, 0::double precision) AS min_cell_v2,
            COALESCE(bus_battery_data_renamed.min_cell_v3, 0::double precision) AS min_cell_v3,
            COALESCE(bus_battery_data_renamed.min_cell_v4, 0::double precision) AS min_cell_v4,
            COALESCE(bus_battery_data_renamed.delta_cell_v1, 0::double precision) AS delta_cell_v1,
            COALESCE(bus_battery_data_renamed.delta_cell_v2, 0::double precision) AS delta_cell_v2,
            COALESCE(bus_battery_data_renamed.delta_cell_v3, 0::double precision) AS delta_cell_v3,
            COALESCE(bus_battery_data_renamed.delta_cell_v4, 0::double precision) AS delta_cell_v4,
            COALESCE(bus_battery_data_renamed.max_cell_t1, 0::double precision) AS max_cell_t1,
            COALESCE(bus_battery_data_renamed.max_cell_t2, 0::double precision) AS max_cell_t2,
            COALESCE(bus_battery_data_renamed.max_cell_t3, 0::double precision) AS max_cell_t3,
            COALESCE(bus_battery_data_renamed.max_cell_t4, 0::double precision) AS max_cell_t4,
            COALESCE(bus_battery_data_renamed.min_cell_t1, 0::double precision) AS min_cell_t1,
            COALESCE(bus_battery_data_renamed.min_cell_t2, 0::double precision) AS min_cell_t2,
            COALESCE(bus_battery_data_renamed.min_cell_t3, 0::double precision) AS min_cell_t3,
            COALESCE(bus_battery_data_renamed.min_cell_t4, 0::double precision) AS min_cell_t4,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 0::double precision
                    ELSE bus_battery_data_renamed.speed
                END AS speed,
            bus_battery_data_renamed."timestamp",
            COALESCE(bus_battery_data_renamed.external_power_status, 'off'::text) AS external_power_status,
            COALESCE(bus_battery_data_renamed.signal_strength, 1::numeric) AS signal_strength,
            COALESCE(bus_battery_data_renamed.can_data_status, 'off'::text) AS can_data_status,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text OR bus_battery_data_renamed.speed = 0::double precision THEN 'off'::text
                    ELSE COALESCE(bus_battery_data_renamed.bus_running_status, 'off'::text)
                END AS bus_running_status,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 'off'::text
                    ELSE COALESCE(bus_battery_data_renamed.regeneration_status, 'off'::text)
                END AS regeneration_status,
            COALESCE(bus_battery_data_renamed.bus_status, 'off'::text) AS bus_status,
            COALESCE(bus_battery_data_renamed.bms_status, 'idle'::text) AS bms_status
           FROM bus_battery_data_renamed
         )
 SELECT bus_battery_data_cte.imei,
    bus_battery_data_cte.status,
    bus_battery_data_cte.bus_number,
    bus_battery_data_cte.depot,
    bus_battery_data_cte.city,
    bus_battery_data_cte.latitude,
    bus_battery_data_cte.longitude,
    bus_battery_data_cte.soc,
    bus_battery_data_cte.soh,
    bus_battery_data_cte.current,
    bus_battery_data_cte.voltage,
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
    bus_battery_data_cte.speed,
    bus_battery_data_cte."timestamp",
    bus_battery_data_cte.external_power_status,
    bus_battery_data_cte.signal_strength,
    bus_battery_data_cte.can_data_status,
    bus_battery_data_cte.bus_running_status,
    bus_battery_data_cte.regeneration_status,
    bus_battery_data_cte.bus_status,
    bus_battery_data_cte.bms_status
   FROM bus_battery_data_cte
     WHERE NOT
 (imei IS 
 NULL OR bus_number IS 
 NULL OR depot IS 
 NULL OR city IS 
 NULL OR latitude IS 
 NULL OR longitude IS 
 NULL OR soc IS 
 NULL OR soh IS 
 NULL OR current IS 
 NULL OR voltage IS 
 NULL OR inlet_temperature IS 
 NULL OR outlet_temperature IS 
 NULL OR max_cell_v1 IS 
 NULL OR max_cell_v2 IS 
 NULL OR max_cell_v3 IS 
 NULL OR max_cell_v4 IS 
 NULL OR min_cell_v1 IS 
 NULL OR min_cell_v2 IS 
 NULL OR min_cell_v3 IS 
 NULL OR min_cell_v4 IS 
 NULL OR delta_cell_v1 IS 
 NULL OR delta_cell_v2 IS 
 NULL OR delta_cell_v3 IS 
 NULL OR delta_cell_v4 IS 
 NULL OR max_cell_t1 IS 
 NULL OR max_cell_t2 IS 
 NULL OR max_cell_t3 IS 
 NULL OR max_cell_t4 IS 
 NULL OR min_cell_t1 IS 
 NULL OR min_cell_t2 IS 
 NULL OR min_cell_t3 IS 
 NULL OR min_cell_t4 IS 
 NULL OR speed IS 
 NULL OR "timestamp" IS 
 NULL OR external_power_status IS 
 NULL OR signal_strength IS 
 NULL OR can_data_status IS 
 NULL OR bus_running_status IS 
 NULL OR regeneration_status IS 
 NULL OR bus_status IS 
 NULL OR bms_status IS 
 NULL OR longitude::numeric <= 0::numeric OR latitude::numeric <= 0::numeric)
       
   ;



--------------------------------------------------------------------
   
--
-- Name: bus_faults_data; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW "public"."bus_faults_data" AS
 WITH "x" AS (
         SELECT "1011a1f3"."IMEI",
            "1011a1f3"."timestamp",
            "unnest"("string_to_array"("1011a1f3"."faults", ','::"text")) AS "fault_description"
           FROM "public"."1011a1f3"
        UNION
         SELECT "1010a1f3"."IMEI",
            "1010a1f3"."timestamp",
            "unnest"("string_to_array"("1010a1f3"."faults", ','::"text")) AS "fault_description"
           FROM "public"."1010a1f3"
        ), "y" AS (
         SELECT "x"."IMEI" AS "imei",
            "btrim"("replace"(("tc_devices"."name")::"text", ' '::"text", ''::"text")) AS "bus_number",
            "x"."fault_description",
            "x"."timestamp" AS "fault_time"
           FROM ("x"
             LEFT JOIN "traccar"."tc_devices" ON ((("x"."IMEI")::"text" = ("tc_devices"."uniqueid")::"text")))
        ), "grouped" AS (
         SELECT DISTINCT ON ("y"."imei", "y"."fault_description") "y"."imei",
            "y"."bus_number",
            "y"."fault_description",
            "y"."fault_time"
           FROM "y"
        ), "fault_data_cte" AS (
         SELECT "grouped"."imei",
            "grouped"."bus_number",
            "grouped"."fault_description",
            "grouped"."fault_time",
            'F345'::"text" AS "fault_code",
            1 AS "fault_level",
            1 AS "fault_duration",
            'open'::"text" AS "fault_status"
           FROM "grouped"
        )
 SELECT "fault_data_cte"."imei",
    "fault_data_cte"."bus_number",
    "fault_data_cte"."fault_description",
    "fault_data_cte"."fault_time",
    "fault_data_cte"."fault_code",
    "fault_data_cte"."fault_level",
    "fault_data_cte"."fault_duration",
    "fault_data_cte"."fault_status"
   FROM "fault_data_cte"
  WITH NO DATA;
