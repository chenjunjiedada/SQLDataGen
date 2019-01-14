drop table data_temporary;
CREATE EXTERNAL TABLE IF NOT EXISTS data_temporary
  ( 
    user_num    STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/datagen/distinct=0.05'
;

