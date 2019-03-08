#!/usr/bin/env bash
#
# Run the collection level aggregation of language features.
#
# Input
#   A file take place on HDFS /join directory (such as result*-language.csv)
#   This file should be the output of record-level language measurement
# Output
#   languages-per-collections-groupped.txt

INPUT=$1

if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
  echo "You should add an input file!"
  exit 1
fi

OUTPUTFILE=languages-per-collections-groupped.txt

# hdfs dfs -rm -r /join/$OUTPUTFILE
# DIR=hdfs://localhost:54310/join/
DIR=..

spark-submit \
   --class LanguagesPerDataProviders \
   --master local[*] \
   target/scala-2.11/europeana-qa_2.11-1.0.jar \
   $DIR $INPUT

#echo Retrieve $OUTPUTFILE
#hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

#rm .*.crc

echo DONE
