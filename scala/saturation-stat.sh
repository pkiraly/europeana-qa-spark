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


OUTPUTFILE=result29-multilingual-saturation-light-statistics

hdfs dfs -rm -r /join/$OUTPUTFILE

spark-submit \
   --class SaturationStat \
   --master local[6] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar

echo Retrieve $OUTPUTFILE
hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

rm .*.crc

echo DONE
