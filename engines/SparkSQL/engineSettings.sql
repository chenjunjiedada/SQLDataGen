set parquet.block.size=134217728;
set dfs.block.size=134217728;
set spark.sql.parquet.enable.bloom.filter=true;
set spark.sql.parquet.bloom.filter.enabled=true;
set parquet.enable.bloom.filter=true;
set parquet.bloom.filter.enabled=true;
---set spark.sql.parquet.bloom.filter.expected.entries=10000,1000000,100000;
---set spark.sql.parquet.bloom.filter.col.name=card_id,device_id,clue_id;


CREATE DATABASE IF NOT EXISTS bloom_filter_data;
use bloom_filter_data;

CREATE TEMPORARY VIEW hive_tbl
USING org.apache.spark.sql.parquet
OPTIONS (
      path "hdfs:///user/hive/warehouse/bloom_filter_data.db/tbl_data_event_1d"
);

CREATE TEMPORARY VIEW hive_bf_tbl
USING org.apache.spark.sql.parquet
OPTIONS (
      path "hdfs:///user/hive/warehouse/bloom_filter_data.db/tbl_data_event_1d_bf"
);

CREATE EXTERNAL TABLE IF NOT EXISTS spark_tbl
  ( record_id                  DECIMAL(23,0)
  , cdr_id                     STRING
  , location_code              DECIMAL(7,0)
  , system_id                  DECIMAL(8,0)
  , clue_id                    STRING
  , hit_element                STRING
  , carrier_code               DECIMAL(2,0)
  , device_id                  STRING
  , cap_time                   TIMESTAMP
  , data_character             STRING
  , netcell_id                 STRING
  , client_mac                 STRING
  , server_mac                 STRING
  , tunnel_type                STRING
  , tunnel_ip_client           STRING
  , tunnel_ip_server           STRING
  , tunnel_id_client           STRING
  , tunnel_id_server           STRING
  , side_one_tunnel_id         STRING
  , side_two_tunnel_id         STRING
  , client_ip                  STRING
  , server_ip                  STRING
  , trans_protocol             STRING
  , client_port                DECIMAL(5,0)
  , server_port                DECIMAL(5,0)
  , app_protocol               STRING
  , client_area                DECIMAL(3,0)
  , server_area                DECIMAL(3,0)
  , language                   STRING
  , stype                      STRING
  , summary                    STRING
  , file_type                  STRING
  , filename                   STRING
  , filesize                   STRING
  , bill_type                  DECIMAL(6,0)
  , ori_user_num               STRING
  , user_num                   STRING
  , user_imsi                  STRING
  , user_imei                  STRING
  , user_belong_area_code      STRING
  , user_belong_country_code   DECIMAL(10,0)
  , user_longitude             DECIMAL(20,8)
  , user_latitude              DECIMAL(20,8)
  , user_msc                   STRING
  , user_base_station          STRING
  , user_curr_area_code        STRING
  , user_curr_country_code     DECIMAL(10,0)
  , user_signal_point          STRING
  , user_ip                    STRING
  , ori_oppo_num               STRING
  , oppo_num                   STRING
  , oppo_imsi                  STRING
  , oppo_imei                  STRING
  , oppo_belong_area_code      STRING
  , oppo_belong_country_code   DECIMAL(10,0)
  , oppo_longitude             DECIMAL(20,8)
  , oppo_latitude              DECIMAL(20,8)
  , oppo_msc                   STRING
  , oppo_base_station          STRING
  , oppo_curr_area_code        STRING
  , oppo_curr_country_code     DECIMAL(10,0)
  , oppo_signal_point          STRING
  , oppo_ip                    STRING
  , ring_time                  TIMESTAMP
  , call_estab_time            TIMESTAMP
  , end_time                   TIMESTAMP
  , call_duration              DECIMAL(10,0)
  , call_status_code           DECIMAL(3,0)
  , dtmf                       STRING
  , ori_other_num              STRING
  , other_num                  STRING
  , roam_num                   STRING
  , send_time                  TIMESTAMP
  , ori_sms_content            STRING
  , ori_sms_code               DECIMAL(3,0)
  , sms_content                STRING
  , sms_num                    DECIMAL(2,0)
  , sms_count                  DECIMAL(2,0)
  , remark                     STRING
  , content_status             DECIMAL(6,0)
  , voc_length                 DECIMAL(9,0)
  , fax_page_count             DECIMAL(6,0)
  , com_over_cause             DECIMAL(6,0)
  , roam_type                  DECIMAL(1,0)
  , sgsn_ad                    STRING
  , ggsn_ad                    STRING
  , pdp_ad                     STRING
  , apn_ni                     STRING
  , apn_oi                     STRING
  , card_id                    STRING
  , time_out                   DECIMAL(1,0)
  , on_time                    TIMESTAMP
  , load_id                    DECIMAL(22,0) 
  )
  STORED AS PARQUET
  LOCATION 'hdfs:///user/hive/warehouse/bloom_filter_data.db/tbl_data_event_1d_bf'
  TBLPROPERTIES ('parquet.enable.bloom.filter'='true',
  'parquet.bloom.filter.enable.column.names'='netcell_id,device_id,clue_id,card_id',
  'parquet.bloom.filter.expected.entries'='100000,1000000,1000000,10000');

