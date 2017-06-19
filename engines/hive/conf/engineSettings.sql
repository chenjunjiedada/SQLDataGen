set user_num='14012030056';
set user_num_in=1,2,3;
set location_code_in=400000,010000,220000;
set parquet.block.size=134217728;
set dfs.block.size=134217728;
set parquet.enable.bloom.filter=true;

CREATE DATABASE IF NOT EXISTS bloom_filter_data;
use bloom_filter_data;
