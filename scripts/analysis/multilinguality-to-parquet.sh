#!/usr/bin/env bash
#
# Convert multilinguality.csv to .parquet file
#
# Input
#   the CSV format of the multilinguality
# Output
#   the parquet file

SECONDS=0

INPUT=$1
OUTPUT=$(echo $INPUT | sed 's/.csv/.parquet/')
echo $OUTPUT

# SPARK_DIR=../spark/spark-2.3.1-bin-hadoop2.7/bin
# $SPARK_DIR/spark-submit \

spark-submit \
  --driver-memory 6g --executor-memory 6g \
  --class de.gwdg.europeanaqa.spark.saturation.MultilingualityToParquet \
  --master local[6] \
  target/scala-2.11/europeana-qa_2.11-1.0.jar \
  $INPUT \
  $OUTPUT

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE

