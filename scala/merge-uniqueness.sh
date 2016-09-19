#!/bin/bash

CSV_FILE=result14.csv
JAR=target/scala-2.10/europeana-qa_2.10-1.0.jar

hdfs dfs -rm -r /join/merged-*.csv

function runSpark {
  spark-submit \
    --class MergeUniqueness \
    --master local[*] \
    $JAR \
    hdfs://localhost:54310/join/$CSV_FILE $1
}


echo "## runSpark 0"
runSpark "0"
#echo "## runSpark 1"
#runSpark "1"
#echo "## runSpark 2020"
#runSpark "2020"
#echo "## runSpark 2021"
#runSpark "2021"
#echo "## runSpark 2022"
#runSpark "2022"
#echo "## runSpark 2023"
#runSpark "2023"
#echo "## runSpark 2024"
#runSpark "2024"
#echo "## runSpark 2025"
#runSpark "2025"
#echo "## runSpark 2026"
#runSpark "2026"
#echo "## runSpark 203"
#runSpark "203"
#echo "## runSpark 204"
#runSpark "204"
#echo "## runSpark 205"
#runSpark "205"
#echo "## runSpark 90"
#runSpark "90"
#echo "## runSpark 91"
#runSpark "91"
#echo "## runSpark 92"
#runSpark "92"

echo "## merge results"
hdfs dfs -getmerge /join/merged-*.csv merged.csv
rm .*.crc

echo "## Done"
