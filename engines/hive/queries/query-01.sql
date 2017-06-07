DROP TABLE IF EXISTS q1_result;
CREATE TABLE q1_result stored as textfile as SELECT * from tbl_data_event_1d WHERE user_num=${hiveconf:user_num};
