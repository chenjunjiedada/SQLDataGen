CREATE EXTERNAL TABLE IF NOT EXISTS data_event_1d_temporary
  ( 
    user_num                    STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION ${location}
;

DROP TABLE IF EXISTS data_event_bf;
CREATE TABLE ata_event_bf
STORED AS parquet
TBLPROPERTIES ('parquet.enable.bloom.filter'='true',
'parquet.bloom.filter.column.names'='user_num',
'parquet.bloom.filter.expected.entries'='50000000',
'parquet.bloom.filter.size'='16777216'
)
AS
SELECT * FROM data_event_1d_temporary
;
