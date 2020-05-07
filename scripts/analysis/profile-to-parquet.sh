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
OUTPUT=$2

#if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
#  echo "You should add an input file!"
#  exit 1
#fi

SECONDS=0
# OUTPUT=$(echo "$INPUT" | sed 's,.csv,.parquet,')

echo "Profiles to parquet"
echo "input: $INPUT"
echo "output: $OUTPUT"

if [[ "$BASE_DIR" != "" ]]; then
  SCALA_DIR=$BASE_DIR/scala
else
  SCALA_DIR=../../scala
fi

# hdfs dfs -rm -r /join/$OUTPUTFILE

spark-submit \
   --executor-memory 3g --driver-memory 3g \
   --class de.gwdg.europeanaqa.spark.profiles.ProfileToParquet \
   --master local[6] \
   $SCALA_DIR/target/scala-2.11/europeana-qa_2.11-1.0.jar \
   $INPUT $OUTPUT ""

#echo Retrieve $OUTPUTFILE
#hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

#rm .*.crc

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
