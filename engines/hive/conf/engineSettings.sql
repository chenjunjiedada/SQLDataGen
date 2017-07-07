set parquet.block.size=134217728;
set dfs.block.size=134217728;
set parquet.enable.bloom.filter=true;
set parquet.bloom.filter.enabled=true;
set hive.optimize.index.filter=true;
set hive.execution.engine=mr;
--set mapreduce.framework.name=local;

CREATE DATABASE IF NOT EXISTS bloom_filter_data;
use bloom_filter_data;
