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

INPUTFILE=result29-multilingual-saturation-light.csv
OUTPUTFILE=result29-multilingual-saturation-light-statistics
HDFS_DIR=hdfs://localhost:54310/join

hdfs dfs -rm -r /join/$OUTPUTFILE

spark-submit \
   --driver-memory 3g \
   --class SaturationStat \
   --master local[6] \
   target/scala-2.11/europeana-qa_2.11-1.0.jar \
   $HDFS_DIR/$INPUTFILE
   $HDFS_DIR/$OUTPUTFILE

echo Retrieve $OUTPUTFILE
hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

rm .*.crc

echo DONE
