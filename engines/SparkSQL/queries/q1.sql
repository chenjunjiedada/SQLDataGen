CREATE TEMPORARY VIEW bf_tbl
USING org.apache.spark.sql.parquet
OPTIONS (
      path "hdfs:///root/BloomFilter/datagen/spark-warehouse/bloom_filter_data.db/sparksql_bf_tbl"
)

