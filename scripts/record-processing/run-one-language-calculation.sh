#!/usr/bin/env bash
#
# Run the record level measure of language features
#
# Input
#   You should specify a .json file which takes place on HDFS /europeana directory
# Output
#   A .csv file with the same name as of the input .json file.

INPUT=$1
if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
  echo "You should add an input file which should exist in HDFS /europeana directory (such as 00101.json)!"
  exit 1
fi

JAR_VERSION=0.7-SNAPSHOT
HDFS=hdfs://localhost:54310
INPUTPATH=$HDFS/europeana/$INPUT
RESULT=$HDFS/result
OUTPUT=$(echo $INPUT | sed 's,.json,-language.csv,')
CRC=.$OUTPUT.crc

echo "Processing language feature measurement for $INPUT ..."

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
  wait 5
fi

JAR=target/europeana-qa-spark-${JAR_VERSION}-jar-with-dependencies.jar

spark-submit --class de.gwdg.europeanaqa.spark.LanguageCount \
  --master local[*] \
  $JAR \
  $INPUTPATH $RESULT \
  data-providers.txt \
  datasets.txt

echo Retrieve $OUTPUT
hdfs dfs -getmerge /result $OUTPUT
rm $CRC

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
fi

echo "Language detection is done!"
