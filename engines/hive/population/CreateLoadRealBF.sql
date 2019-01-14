DROP TABLE IF EXISTS data_temporary;
CREATE EXTERNAL TABLE IF NOT EXISTS data_temporary
  ( 
    user_num                    STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/datagen/distinct=0.25'
;

DROP TABLE IF EXISTS data_event_bf_disctinct_0_25;
CREATE TABLE data_event_bf_distinct_0_25
STORED AS parquet
TBLPROPERTIES ('parquet.enable.bloom.filter'='true',
'parquet.bloom.filter.column.names'='user_num',
'parquet.bloom.filter.expected.entries'='524288',
'parquet.bloom.filter.size'='524288'
)
AS
SELECT * FROM data_temporary
;
