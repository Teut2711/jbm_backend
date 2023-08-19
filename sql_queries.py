bus_data_cte = """
WITH SUB_BUS_DATA AS (
            SELECT "IMEI", "traccar"."tc_devices"."name",
            CASE
            WHEN "status__full_charged" = '1.000000' THEN 'Fully Charged'
            END "full-charged",
            CASE
            WHEN "status__BMS" = '1.000000' THEN 'Idle'
            WHEN "status__BMS" = '2.000000' THEN 'Charging'
            WHEN "status__BMS" = '3.000000' THEN 'Discharging'
            END "BMS"
            FROM "status__BMS" INNER JOIN "traccar"."tc_devices" ON "traccar"."tc_devices"."uniqueid"::TEXT = "status__BMS"."IMEI"::TEXT --GROUP BY 3, 4;
                        WHERE name != CAST("IMEI" AS TEXT) ),
BUS_DATA AS (
    SELECT SUB_BUS_DATA.name, SUB_BUS_DATA."IMEI",ARRAY_REMOVE(ARRAY[SUB_BUS_DATA."full-charged", SUB_BUS_DATA."BMS"], NULL) as status, tp.latitude, tp.longitude
    FROM SUB_BUS_DATA
    JOIN traccar.tc_devices AS td ON td.name = SUB_BUS_DATA.name
    JOIN traccar.tc_positions AS tp ON td.id = tp.deviceid
    WHERE (SUB_BUS_DATA.name, tp.devicetime) IN (
        SELECT SUB_BUS_DATA.name, MAX(tp.devicetime) AS max_devicetime
        FROM SUB_BUS_DATA
        JOIN traccar.tc_devices AS td ON td.name = SUB_BUS_DATA.name
        JOIN (
        SELECT *
        FROM traccar.tc_positions
        WHERE latitude != 0 AND longitude != 0
    ) AS tp ON td.id = tp.deviceid
        GROUP BY SUB_BUS_DATA.name
    )
)
"""
