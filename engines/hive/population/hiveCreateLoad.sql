drop table data_event_1d_temporary;
CREATE EXTERNAL TABLE IF NOT EXISTS data_event_1d_temporary
  ( user_num    STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION ${location}
;

DROP TABLE IF EXISTS data_event;
CREATE TABLE data_event
STORED AS parquet
AS
SELECT * FROM data_event_1d_temporary
;

