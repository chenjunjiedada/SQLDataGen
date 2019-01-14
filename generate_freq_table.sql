set parquet.block.size=134217728;
set parquet.enable.bloom.filter=true;
set parquet.bloom.filter.enabled=true;
set parquet.filter.bloom.enabled=true;
set hive.optimize.index.filter=true;
set hive.optimize.ppd=true;
use bloom_filter_data;
set hive.execution.engine=mr;
--set mapreduce.framework.name=local;
--show create table ata_event_bf;

DROP TABLE IF EXISTS compact_freq_table_0_05;
CREATE TABLE compact_freq_table_0_05 (
      value STRING,
      freq INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
     
with freq_table as 
(
        select  user_num, count(*) as freq from data_temporary group by user_num order by freq desc
),
sorted_freq_table as
(
    select user_num, freq, ROW_NUMBER() over (partition by freq) as row_num from freq_table
)
insert into table compact_freq_table_0_05
select user_num, freq from sorted_freq_table where row_num=1;

