#!/bin/bash

CSV_FILE=result11.csv
JAR=target/scala-2.10/europeana-qa_2.10-1.0.jar

hdfs dfs -rm -r /join/merged-*.csv

function runSpark {
  spark-submit \
    --class MergeUniqueness \
    --master local[*] \
    $JAR \
    hdfs://localhost:54310/join/$CSV_FILE $1
}

runSpark "0"
runSpark "1"
runSpark "2020"
runSpark "2021"
runSpark "2022"
runSpark "2023"
runSpark "2024"
runSpark "2025"
runSpark "2026"
runSpark "203"
runSpark "204"
runSpark "205"
runSpark "90"
runSpark "91"
runSpark "92"

hdfs dfs -getmerge /join/merged-*.csv merged.csv
rm .*.crc
