#!/usr/bin/env bash
#
# Run top level cardinality and frequency analysis
#
# Input
#   a file take place on HDFS /join directory
# Output:
#   cardinality.csv, frequency.csv

INPUT=$1

if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
  echo "You should add an input file!"
  exit 1
fi

hdfs dfs -rm -r /join/cardinality.csv
hdfs dfs -rm -r /join/frequency.csv


spark-submit \
   --class Cardinality \
   --master local[*] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar \
   hdfs://localhost:54310/join/$INPUT \
   hdfs://localhost:54310/join/cardinality.csv \
   hdfs://localhost:54310/join/frequency.csv

echo Retrieve cardinality.csv
hdfs dfs -getmerge /join/cardinality.csv cardinality.csv

echo Retrieve frequency.csv
hdfs dfs -getmerge /join/frequency.csv frequency.csv
rm .*.crc

echo DONE
