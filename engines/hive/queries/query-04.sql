DROP TABLE IF EXISTS q4_result;
CREATE TABLE q4_result stored as textfile as SELECT * from tbl_data_event_1d WHERE location_code in (${hiveconf:location_code_in});
