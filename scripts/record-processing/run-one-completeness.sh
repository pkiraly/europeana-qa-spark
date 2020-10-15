#!/usr/bin/env bash
#
# Run the record level measure of main features
#
# Input
#   You should specify a .json file which takes place on HDFS /europeana directory
# Output
#   A .csv file with the same name as of the input .json file.

INPUT=$1
SKIP_FLAG=$2
if [[ ("$#" -ne 1 && "$#" -ne 2) || ("$INPUT" == "") ]]; then
  echo "You should add an input file which should exist in HDFS /europeana directory (such as 00101.json)!"
  exit 1
fi
if [[ ("$SKIP_FLAG" != "checkSkippableCollections") ]]; then
  SKIP_FLAG=""
fi

JAR_VERSION=0.7-SNAPSHOT
HDFS=hdfs://localhost:54310
INPUTPATH=$HDFS/europeana/$INPUT
RESULT=$HDFS/result
OUTPUT=$(echo $INPUT | sed 's,.json,.csv,')
CRC=.$OUTPUT.crc

echo "Processing main feature measurement for $INPUT ..."

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
  wait 5
fi

JAR=target/europeana-qa-spark-${JAR_VERSION}-jar-with-dependencies.jar

spark-submit --class de.gwdg.europeanaqa.spark.CompletenessCount \
  --master local[*] \
  $JAR \
  $INPUTPATH $RESULT \
  data-providers.txt datasets.txt \
  $SKIP_FLAG

echo Retrieve $OUTPUT
hdfs dfs -getmerge /result $OUTPUT
rm $CRC

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
fi

echo "Completeness count is done!"
