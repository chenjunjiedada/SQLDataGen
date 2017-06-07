DROP TABLE IF EXISTS q2_result;
CREATE TABLE q2_result stored as textfile as SELECT * from tbl_data_event_1d WHERE user_num in (${hiveconf:user_num_in});
