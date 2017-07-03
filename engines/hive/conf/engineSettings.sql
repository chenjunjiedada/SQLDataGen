set parquet.block.size=134217728;
set dfs.block.size=134217728;
set parquet.enable.bloom.filter=true;
set parquet.bloom.filter.enabled=true;
set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;

CREATE DATABASE IF NOT EXISTS bloom_filter_data;
use bloom_filter_data;
