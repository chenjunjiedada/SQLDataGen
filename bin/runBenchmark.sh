#!/bin/bash
export BENCH_MARK_HOME=`cd $(dirname ${BASH_SOURCE[0]})/..&& pwd`
#while read line;do
#    eval "$line"
#done < $BENCH_MARK_HOME/engines/hive/conf/engineSettings.conf
source $BENCH_MARK_HOME/engines/hive/conf/engineSettings.conf
echo $CLEAN_DATA
echo $DATA_GENERATION
echo $CREATETABLE_LOADDATA
echo $POWER_TEST
if [ $CLEAN_DATA = true ]
then
    hive -e 'drop database if exists tbl_data cascade'
    hadoop fs -rm -r /user/hive/warehouse/tbl_data.db
fi
if [ $DATA_GENERATION = true ]
then
    echo 'this function has not done'
fi
if [ $CREATETABLE_LOADDATA = true ]
then
    hive -i $BENCH_MARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCH_MARK_HOME/engines/hive/population/hiveCreateLoad.sql
fi
if [ $POWER_TEST = true ]
then
    hive -i $BENCH_MARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCH_MARK_HOME/engines/hive/queries/query-01.sql 2> $BENCH_MARK_HOME/result/test1.log 1>/dev/null
    echo "test #1">$BENCH_MARK_HOME/result/result.log
    grep "Time taken" $BENCH_MARK_HOME/result/test1.log >>$BENCH_MARK_HOME/result/result.log
    hive -i $BENCH_MARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCH_MARK_HOME/engines/hive/queries/query-02.sql 2> $BENCH_MARK_HOME/result/test2.log 1>/dev/null
        echo "test #2">$BENCH_MARK_HOME/result/result.log
        grep "Time taken" $BENCH_MARK_HOME/result/test2.log >>$BENCH_MARK_HOME/result/result.log
    hive -i $BENCH_MARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCH_MARK_HOME/engines/hive/queries/query-03.sql 2> $BENCH_MARK_HOME/result/test3.log 1>/dev/null
        echo "test #3">$BENCH_MARK_HOME/result/result.log
        grep "Time taken" $BENCH_MARK_HOME/result/test3.log >>$BENCH_MARK_HOME/result/result.log
    hive -i $BENCH_MARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCH_MARK_HOME/engines/hive/queries/query-04.sql 2> $BENCH_MARK_HOME/result/test4.log 1>/dev/null
        echo "test #4">$BENCH_MARK_HOME/result/result.log
        grep "Time taken" $BENCH_MARK_HOME/result/test4.log >>$BENCH_MARK_HOME/result/result.log
    hive -i $BENCH_MARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCH_MARK_HOME/engines/hive/queries/query-05.sql 2> $BENCH_MARK_HOME/result/test5.log 1>/dev/null
        echo "test #5">$BENCH_MARK_HOME/result/result.log
        grep "Time taken" $BENCH_MARK_HOME/result/test5.log >>$BENCH_MARK_HOME/result/result.log

fi