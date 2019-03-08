#!/usr/bin/env bash
#
# Run the top level aggregation of language features.
#
# Input
#   A file take place on HDFS /join directory (such as result*-language.csv)
#   This file should be the output of record-level language measurement
# Output
#   languages.csv

INPUT=$1

if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
  echo "You should add an input file!"
  exit 1
fi

DIR=..
# DIR=hdfs://localhost:54310/join/
OUTPUTFILE=languages.csv

hdfs dfs -rm -r /join/$OUTPUTFILE

spark-submit \
   --class Languages \
   --master local[*] \
   target/scala-2.11/europeana-qa_2.11-1.0.jar \
   $DIR $INPUT

echo Retrieve $OUTPUTFILE
hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

rm .*.crc

echo DONE
