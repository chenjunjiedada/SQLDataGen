# SQLDataGen
SQLDataGen generate data on HDFS according to SQL schema.

# Steps
1. configure data you want to generate
    edit conf/global.conf

2. configure data schema you want to generate
    edit engine/$NAME/conf/column.properties

3. generate data
    run bin/runBenchmark.sh
