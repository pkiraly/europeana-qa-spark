#!/usr/bin/env bash
#
# Run the record level measure of multilingual saturation features
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

JAR_VERSION=0.4-SNAPSHOT
HDFS=hdfs://localhost:54310
INPUTPATH=$HDFS/europeana/$INPUT
RESULT=$HDFS/result
OUTPUT=$(echo $INPUT | sed 's,.json,-multilingual-saturation.csv,')
OUTPUTCRC=.$OUTPUT.crc
HEADER=header-multilingual-saturation.csv
HEADEROUTPUT=/join/$HEADER
HEADERCRC=.$HEADER.crc

echo "Processing multilingual saturation measurement for $INPUT ..."

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
  sleep 5
fi

if hdfs dfs -test -d $HEADEROUTPUT; then
  hdfs dfs -rm -r -f -skipTrash $HEADEROUTPUT
  sleep 2
fi

JAR=target/europeana-qa-spark-${JAR_VERSION}-jar-with-dependencies.jar

spark-submit --class de.gwdg.europeanaqa.spark.MultilingualSaturation \
  --master local[*] \
  $JAR \
  $INPUTPATH $RESULT \
  $HDFS$HEADEROUTPUT \
  data-providers.txt \
  datasets.txt

echo Retrieve $OUTPUT
hdfs dfs -getmerge /result $OUTPUT
sleep 2
rm -f $OUTPUTCRC

echo Retrieve $HEADER
hdfs dfs -getmerge $HEADEROUTPUT $HEADER
sleep 2
rm -f $HEADERCRC

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
  sleep 5
fi

if hdfs dfs -test -d $HEADEROUTPUT; then
  hdfs dfs -rm -r -f -skipTrash $HEADEROUTPUT
  sleep 2
fi

echo "Multilingual saturation is done!"
