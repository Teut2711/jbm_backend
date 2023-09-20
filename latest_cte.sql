```Faults
 WITH x AS (
         SELECT status."IMEI"::text AS imei,
            array_remove(string_to_array(status._faults, ','::text, ''::text), NULL::text) AS all_faults
           FROM status
        ), y AS (
         SELECT x.imei,
            x.all_faults
           FROM x
          WHERE x.all_faults IS NOT NULL AND cardinality(x.all_faults) > 0
        ), z AS (
         SELECT y.imei,
            btrim(replace(tc_devices.name::text, ' '::text, ''::text)) AS bus_number,
            split_part(t.element, '|'::text, 1) AS fault_time,
            split_part(t.element, '|'::text, 2) AS fault_code,
            split_part(t.element, '|'::text, 3) AS fault_description,
            split_part(t.element, '|'::text, 4) AS fault_level,
            now()::timestamp without time zone - split_part(t.element, '|'::text, 1)::timestamp without time zone AS fault_duration,
            'open'::text AS fault_status
           FROM y
             LEFT JOIN traccar.tc_devices ON y.imei = tc_devices.uniqueid::text
             CROSS JOIN LATERAL unnest(y.all_faults) t(element)
        )
 SELECT z.imei,
    z.bus_number,
    z.fault_time,
    z.fault_code,
    z.fault_description,
    z.fault_level,
    z.fault_duration::text AS fault_duration,
    z.fault_status
   FROM z;
   ```

```BUSES
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
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '0.000000'::text THEN 'partially-charged'::text
                    WHEN last(c00a1f3."B2V_FullChrg", c00a1f3."timestamp") = '1.000000'::text THEN 'full-charged'::text
                    ELSE 'Unknown'::text
                END AS status__battery_pack__full_charge,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '2.000000'::text THEN 'charging'::text
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text THEN 'discharging'::text
                    ELSE 'Idle'::text
                END AS status__battery_pack__current_polarity,
                CASE
                    WHEN last(c00a1f3."timestamp", c00a1f3."timestamp") < (now() - '01:00:00'::interval) THEN 'idle'::text
                    ELSE 'Online'::text
                END AS status__position_tracker_advanced__connection,
            last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") AS current,
            last(c00a1f3."B2V_HVP", c00a1f3."timestamp") AS voltage,
                CASE
                    WHEN last(c00a1f3."B2V_BMSSta", c00a1f3."timestamp") = '3.000000'::text AND last(c00a1f3."B2V_TotalI", c00a1f3."timestamp") < 0::numeric THEN 'regenerative-braking'::text
                    ELSE 'Unknown'::text
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
            battery_pack__18fff345.inlet_temperature::numeric AS inlet_temperature,
            battery_pack__18fff345.outlet_temperature::numeric AS outlet_temperature
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
                    WHEN battery_pack."timestamp" IS NULL THEN array_remove(ARRAY['idle'::text], NULL::text)
                    ELSE array_remove(ARRAY[battery_pack.status__position_tracker_advanced__connection], NULL::text)
                END AS status,
            battery_pack.status__battery_pack__full_charge,
            battery_pack.status__battery_pack__current_polarity,
            battery_pack.status__position_tracker_advanced__connection,
            battery_pack.status__vehicle_control_unit__regenerative_braking,
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
            last(tc_positions.speed, tc_positions.fixtime)::numeric AS speed
           FROM traccar.tc_positions
          WHERE tc_positions.latitude <> 0::double precision AND tc_positions.longitude <> 0::double precision
          GROUP BY tc_positions.deviceid
        ), asset AS (
         SELECT row_number() OVER (ORDER BY vehicle."IMEI") AS "#",
            now() AS "updatedAt",
            vehicle."IMEI",
            vehicle."timestamp" AS timestamp_e,
            vehicle.status,
            vehicle.status__battery_pack__full_charge,
            vehicle.status__battery_pack__current_polarity,
            vehicle.status__position_tracker_advanced__connection,
            vehicle.status__vehicle_control_unit__regenerative_braking,
                CASE
                    WHEN vehicle.status__position_tracker_advanced__connection = 'idle'::text THEN 'idle'::text
                    WHEN vehicle."timestamp" < (now() - '1 day'::interval) THEN 'idle'::text
                    ELSE vehicle.status__battery_pack__current_polarity
                END AS status0,
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
         SELECT battery_pack__1010a1f3."IMEI" AS imei,
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
            btrim(replace(asset.name::text, ' '::text, ''::text)) AS bus_number,
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
            asset."RSSI" AS signal_strength,
                CASE
                    WHEN asset."OBD2" IS NULL THEN 'off'::text
                    WHEN asset."OBD2" = true THEN 'on'::text
                    ELSE 'off'::text
                END AS can_data_status,
                CASE
                    WHEN asset.speed IS NULL THEN 'off'::text
                    WHEN asset.speed::double precision::integer <= 0 THEN 'off'::text
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
            COALESCE(bus_battery_data_renamed.bus_number, ''::text) AS bus_number,
            COALESCE(bus_battery_data_renamed.depot, ''::text) AS depot,
            COALESCE(bus_battery_data_renamed.city, ''::text) AS city,
            bus_battery_data_renamed.latitude,
            bus_battery_data_renamed.longitude,
            COALESCE(bus_battery_data_renamed.soc, 0::numeric) AS soc,
            COALESCE(bus_battery_data_renamed.soh, 0::numeric) AS soh,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 0::numeric
                    ELSE COALESCE(bus_battery_data_renamed.current, 0::numeric)
                END AS current,
            COALESCE(bus_battery_data_renamed.voltage, 0::numeric) AS voltage,
            COALESCE(bus_battery_data_renamed.inlet_temperature, 0::numeric) AS inlet_temperature,
            COALESCE(bus_battery_data_renamed.outlet_temperature, 0::numeric) AS outlet_temperature,
            COALESCE(bus_battery_data_renamed.max_cell_v1, 0::numeric) AS max_cell_v1,
            COALESCE(bus_battery_data_renamed.max_cell_v2, 0::numeric) AS max_cell_v2,
            COALESCE(bus_battery_data_renamed.max_cell_v3, 0::numeric) AS max_cell_v3,
            COALESCE(bus_battery_data_renamed.max_cell_v4, 0::numeric) AS max_cell_v4,
            COALESCE(bus_battery_data_renamed.min_cell_v1, 0::numeric) AS min_cell_v1,
            COALESCE(bus_battery_data_renamed.min_cell_v2, 0::numeric) AS min_cell_v2,
            COALESCE(bus_battery_data_renamed.min_cell_v3, 0::numeric) AS min_cell_v3,
            COALESCE(bus_battery_data_renamed.min_cell_v4, 0::numeric) AS min_cell_v4,
            COALESCE(bus_battery_data_renamed.delta_cell_v1, 0::numeric) AS delta_cell_v1,
            COALESCE(bus_battery_data_renamed.delta_cell_v2, 0::numeric) AS delta_cell_v2,
            COALESCE(bus_battery_data_renamed.delta_cell_v3, 0::numeric) AS delta_cell_v3,
            COALESCE(bus_battery_data_renamed.delta_cell_v4, 0::numeric) AS delta_cell_v4,
            COALESCE(bus_battery_data_renamed.max_cell_t1, 0::numeric) AS max_cell_t1,
            COALESCE(bus_battery_data_renamed.max_cell_t2, 0::numeric) AS max_cell_t2,
            COALESCE(bus_battery_data_renamed.max_cell_t3, 0::numeric) AS max_cell_t3,
            COALESCE(bus_battery_data_renamed.max_cell_t4, 0::numeric) AS max_cell_t4,
            COALESCE(bus_battery_data_renamed.min_cell_t1, 0::numeric) AS min_cell_t1,
            COALESCE(bus_battery_data_renamed.min_cell_t2, 0::numeric) AS min_cell_t2,
            COALESCE(bus_battery_data_renamed.min_cell_t3, 0::numeric) AS min_cell_t3,
            COALESCE(bus_battery_data_renamed.min_cell_t4, 0::numeric) AS min_cell_t4,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 0::numeric
                    ELSE COALESCE(bus_battery_data_renamed.speed, 0::numeric)
                END AS speed,
            bus_battery_data_renamed."timestamp",
            COALESCE(bus_battery_data_renamed.external_power_status, 'off'::text) AS external_power_status,
            COALESCE(bus_battery_data_renamed.signal_strength::numeric, 1::numeric) AS signal_strength,
            COALESCE(bus_battery_data_renamed.can_data_status, 'off'::text) AS can_data_status,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text OR bus_battery_data_renamed.speed = 0::numeric THEN 'off'::text
                    ELSE COALESCE(bus_battery_data_renamed.bus_running_status, 'off'::text)
                END AS bus_running_status,
                CASE
                    WHEN bus_battery_data_renamed.bms_status = 'idle'::text THEN 'off'::text
                    ELSE COALESCE(bus_battery_data_renamed.regeneration_status, 'off'::text)
                END AS regeneration_status,
            COALESCE(bus_battery_data_renamed.bus_status, 'off'::text) AS bus_status,
            COALESCE(bus_battery_data_renamed.bms_status, 'idle'::text) AS bms_status
           FROM bus_battery_data_renamed
          WHERE NOT (bus_battery_data_renamed.status IS NULL OR bus_battery_data_renamed.bus_number IS NULL OR bus_battery_data_renamed.depot IS NULL OR bus_battery_data_renamed.city IS NULL OR bus_battery_data_renamed.latitude IS NULL OR bus_battery_data_renamed.longitude IS NULL)
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
   ```

