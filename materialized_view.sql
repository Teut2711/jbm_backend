CREATE UNIQUE INDEX unique_imei_index ON bus_battery_data ("imei");
CREATE UNIQUE INDEX unique_fault_index ON bus_faults_data ("imei", "fault_code", "start_time");

