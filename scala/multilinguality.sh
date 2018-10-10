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
PHASE=$2

#if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
#  echo "You should add an input file!"
#  exit 1
#fi

# --class de.gwdg.europeanaqa.spark.saturation.MultilingualityWithHistogramWithMapReduce \
# --executor-memory 6g \
# --conf spark.local.dir=/projects/pkiraly/tmp \

spark-submit \
  --driver-memory 3g \
  --class de.gwdg.europeanaqa.spark.saturation.MultilingualityFromParquet \
  --master local[6] \
  target/scala-2.11/europeana-qa_2.11-1.0.jar \
  $INPUT $PHASE

# echo Retrieve $OUTPUTFILE
# hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE

# rm .*.crc

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
