# SQLDataGen
SQLDataGen is use to generate interactive workload for benchmark the interactive query performance of database.

##How to use SQLDataGen
This project needs to be run on a cluster which installs hadoop,hive.
First, user should run main function in src/main/java/generator/SchemaGenerator.java to generate data on hdfs.
Then user can run bin/runBenchmark.sh to do the five tests automatically.

##Change Properties
User can change scale,proportion of null,data outputdir in conf/datagen.properties.
User can change which steps to run in engines/hive/conf/engineSettings.conf and test-data in five querys in engines/hive/conf/engineSettings.sql