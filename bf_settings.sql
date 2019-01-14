set parquet.block.size=33554432;
--set parquet.block.size=134217728;
set parquet.enable.bloom.filter=true;
set parquet.bloom.filter.enabled=true;
set parquet.filter.bloom.enabled=true;
set hive.optimize.index.filter=true;
set hive.optimize.ppd=true;
--set mapreduce.input.fileinputformat.split.maxsize=134217728;

--set parquet.filter.dictionary.enable=true;
use bloom_filter_data;
--set hive.exec.parallel.thread.number=8;
--set hive.execution.engine=mr;
--set parquet.dictionary.page.size=134217728;
set mapreduce.framework.name=local;
--show create table ata_event_bf;
--- select * from ata_event_bf limit 10;
--select user_num, count(*) as freq from data_event group by user_num order by freq desc limit 5;

--DROP TABLE IF EXISTS compact_freq_table_0_5;
--CREATE TABLE compact_freq_table_0_5 (
--      value STRING,
--      freq INT
--)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
--
--     
--with freq_table as 
--(
--        select  user_num, count(*) as freq from data_event_bf_distinct_0_5 group by user_num order by freq desc
--),
--sorted_freq_table as
--(
--    select user_num, freq, ROW_NUMBER() over (partition by freq) as row_num from freq_table
--)
--insert into table compact_freq_table_0_5
--select user_num, freq from sorted_freq_table where row_num=1;


--select count(*) from data_event where user_num='9999996975603';
--select count(*) from data_event where user_num='5877771264632';
