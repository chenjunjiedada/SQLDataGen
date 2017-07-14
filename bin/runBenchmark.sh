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
    #$HIVE_HOME/bin/hive -e "drop database if exists $DATABASE cascade"
    $HADOOP_HOME/bin/hadoop fs -rm -r -f $OUTPUT_DIRECTORY
fi

if [ $DATA_GENERATION = true ]
then
    echo 'Generate data'
    export MAVEN_OPTS="-Xms256m -Xmx4g"
    mvn clean install -q && mvn exec:java
fi

if [ $LOADDATA = true ]
then
    if [ $ENGINE == "hive" ] 
    then
        echo "Load data into tbl_data_event"
        sed -e "s#\${location}#\'$OUTPUT_DIRECTORY\'#g" $BENCHMARK_HOME/engines/hive/population/hiveCreateLoad.sql > $BENCHMARK_HOME/engines/hive/population/CreateLoadReal.sql
        $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/population/CreateLoadReal.sql
        echo "Load data into tbl_data_event_bf"
        sed -e "s#\${location}#\'$OUTPUT_DIRECTORY\'#g" $BENCHMARK_HOME/engines/hive/population/hiveCreateLoadBF.sql >  $BENCHMARK_HOME/engines/hive/population/CreateLoadRealBF.sql
        $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $BENCHMARK_HOME/engines/hive/population/CreateLoadRealBF.sql
    fi
fi

if [ $SAMPLING = true ]
then
    echo "Generate values for search "
    for file in $BENCHMARK_HOME/engines/hive/sample/*
    do
        if [[ -f $file ]]; then
            mkdir -p $BENCHMARK_HOME/result
            log=$BENCHMARK_HOME/result/$(basename "$file" ".sql")".log"
            $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $file 1>$log 2>&1
        fi
    done
fi

if [ $POWER_TEST = true ]
then
    echo "TODO"
    #for file in $BENCHMARK_HOME/engines/hive/queries/*
    #do
    #    if [[ -f $file ]]; then
    #        mkdir -p $BENCHMARK_HOME/result
    #        log=$BENCHMARK_HOME/result/$(basename "$file" ".sql")".log"
    #        $HIVE_HOME/bin/hive -i $BENCHMARK_HOME/engines/hive/conf/engineSettings.sql -f $file 1>$log 2>&1
    #    fi
    #done
fi
