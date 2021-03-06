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

OUTPUTFILE=saturation-per-collections.csv

hdfs dfs -rm -r /join/$OUTPUTFILE

spark-submit \
   --class SaturationPerCollections \
   --master local[*] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar \
   hdfs://localhost:54310/join/ $INPUT $OUTPUTFILE

echo Retrieve $OUTPUTFILE
hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

rm .*.crc

echo DONE
