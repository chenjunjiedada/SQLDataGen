drop table data_temporary;
CREATE EXTERNAL TABLE IF NOT EXISTS data_temporary
  ( user_num    STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/datagen/distinct=0.05'
;

DROP TABLE IF EXISTS data_event_dic_distinct_0_05;
CREATE TABLE data_event_dic_distinct_0_05
STORED AS parquet
AS
SELECT * FROM data_temporary
;

