DROP TABLE IF EXISTS q5_result;
CREATE TABLE q5_result stored as textfile as SELECT DISTINCT user_num from tbl_data_event_1d WHERE location_code in (${hiveconf:location_code_in});
