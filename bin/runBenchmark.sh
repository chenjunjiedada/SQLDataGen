#!/bin/bash
export BENCHMARK_HOME=`cd $(dirname ${BASH_SOURCE[0]})/..&& pwd`

source $BENCHMARK_HOME/conf/global.conf
if [ -z $OUTPUT_DIRECTORY ]
then
    echo "Please specify the datagen output directory"
fi

# Pass Global configurations to engine specific configuration.
sed -i "s#^\( *datagen\.output\.directory *\).*#\1$OUTPUT_DIRECTORY#g" $BENCHMARK_HOME/engines/$ENGINE/conf/engineSettings.conf
sed -i "s#^\( *datagen\.scale *\).*#\1$SCALE#g" $BENCHMARK_HOME/engines/$ENGINE/conf/engineSettings.conf

if [ $CLEAN_DATA = true ]
then
    $HIVE_HOME/bin/hive -e "drop database if exists $DATABASE cascade"
    $HADOOP_HOME/bin/hadoop fs -rm -r -f $OUTPUT_DIRECTORY
fi

if [ $DATA_GENERATION = true ]
then
    echo 'Generate data'
    mvn clean install && mvn exec:java
fi

if [ $CREATETABLE_LOADDATA = true ]
then
    echo "Load data into database"
    $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/population/hiveCreateLoad.sql
fi
if [ $POWER_TEST = true ]
then
    for file in $BENCHMARK_HOME/engines/hive/queries/*
    do
        if [[ -f $file ]]; then
            mkdir -p $BENCHMARK_HOME/result
            log=$BENCHMARK_HOME/result/$(basename "$file" ".sql")".log"
            $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $file 1>$log 2>&1
        fi
    done
fi
