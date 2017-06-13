#!/bin/bash
export BENCHMARK_HOME=`cd $(dirname ${BASH_SOURCE[0]})/..&& pwd`

source $BENCHMARK_HOME/engines/hive/conf/engineSettings.conf

if [ $CLEAN_DATA = true ]
then
#    $HIVE_HOME/bin/hive -e 'drop database if exists tbl_data cascade'
    $HADOOP_HOME/bin/hadoop fs -rm -r -f $DATAGEN_LOCATION
fi
if [ $DATA_GENERATION = true ]
then
    echo 'Generate data'
    #mvn exec:java
fi
if [ $CREATETABLE_LOADDATA = true ]
then
    echo "Load engine settings"
    #$HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/population/hiveCreateLoad.sql
fi
if [ $POWER_TEST = true ]
then
    for file in $BENCHMARK_HOME/engines/hive/queries/*
    do
        if [[ -f $file ]]; then
            log=$BENCHMARK_HOME/result/$(basename "$file" ".sql")".log"
            $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $file 1>$log 2>&1
        fi
    done
    #hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/queries/query-01.sql 2> $BENCHMARK_HOME/result/test1.log 1>/dev/null
    #echo "test #1">$BENCHMARK_HOME/result/result.log
    #grep "Time taken" $BENCHMARK_HOME/result/test1.log >>$BENCHMARK_HOME/result/result.log
    #hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/queries/query-02.sql 2> $BENCHMARK_HOME/result/test2.log 1>/dev/null
    #    echo "test #2">$BENCHMARK_HOME/result/result.log
    #    grep "Time taken" $BENCHMARK_HOME/result/test2.log >>$BENCHMARK_HOME/result/result.log
    #hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/queries/query-03.sql 2> $BENCHMARK_HOME/result/test3.log 1>/dev/null
    #    echo "test #3">$BENCHMARK_HOME/result/result.log
    #    grep "Time taken" $BENCHMARK_HOME/result/test3.log >>$BENCHMARK_HOME/result/result.log
    #hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/queries/query-04.sql 2> $BENCHMARK_HOME/result/test4.log 1>/dev/null
    #    echo "test #4">$BENCHMARK_HOME/result/result.log
    #    grep "Time taken" $BENCHMARK_HOME/result/test4.log >>$BENCHMARK_HOME/result/result.log
    #hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/queries/query-05.sql 2> $BENCHMARK_HOME/result/test5.log 1>/dev/null
    #    echo "test #5">$BENCHMARK_HOME/result/result.log
    #    grep "Time taken" $BENCHMARK_HOME/result/test5.log >>$BENCHMARK_HOME/result/result.log

fi
