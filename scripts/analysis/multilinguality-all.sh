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
KEEP_DIRS=$2
SECONDS=0

if [[ ("$KEEP_DIRS" == "--keep-dirs") ]]; then
  DO_KEEP=1
else
  DO_KEEP=0
fi

printf "Do keep: %d\n" $DO_KEEP

if [[ "$BASE_DIR" = "" ]]; then
  BASE_DIR=$(readlink -e $(dirname $0)/../..)
fi
source $BASE_DIR/base-dirs.sh

#if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
#  echo "You should add an input file!"
#  exit 1
#fi

INPUT_PATH=$(readlink -e $INPUT)
INPUT_DIR=$(dirname $INPUT_PATH)
echo "INPUT_DIR: $INPUT_DIR"

SCALA_DIR=$BASE_DIR/scala
LOG_DIR=$BASE_DIR/logs
OUTPUT_DIR=$BASE_DIR/output

#  --executor-memory 6g \
CLASS=de.gwdg.europeanaqa.spark.saturation.MultilingualityFromParquet
JAR=$SCALA_DIR/target/scala-2.11/europeana-qa_2.11-1.0.jar
MEMORY=3g
CORES=6
CONF="spark.local.dir=$SPARK_TEMP_DIR"

COMMON_PARAMS="--driver-memory $MEMORY --class $CLASS --master local[$CORES] --conf $CONF $JAR $INPUT"
echo $COMMON_PARAMS

phase=prepare
time=$(date +"%T")
echo "$time> 1/6 $phase phase ($LOG_DIR/multilinguality-analysis-$phase.log)"
spark-submit $COMMON_PARAMS "$phase" &> $LOG_DIR/multilinguality-analysis-$phase.log

phase=statistics
time=$(date +"%T")
echo "$time> 2/6 $phase phase ($LOG_DIR/multilinguality-analysis-$phase.log)"
spark-submit $COMMON_PARAMS "$phase" &> $LOG_DIR/multilinguality-analysis-$phase.log

phase=median
time=$(date +"%T")
echo "$time> 3/6 $phase phase ($LOG_DIR/multilinguality-analysis-$phase.log)"
spark-submit $COMMON_PARAMS "$phase" &> $LOG_DIR/multilinguality-analysis-$phase.log

phase=histogram
time=$(date +"%T")
echo "$time> 4/6 $phase phase ($LOG_DIR/multilinguality-analysis-$phase.log)"
spark-submit $COMMON_PARAMS "$phase" &> $LOG_DIR/multilinguality-analysis-$phase.log

phase=join
time=$(date +"%T")
echo "$time> 5/6 $phase phase ($LOG_DIR/multilinguality-analysis-$phase.log)"
spark-submit $COMMON_PARAMS "$phase" &> $LOG_DIR/multilinguality-analysis-$phase.log

time=$(date +"%T")
echo "$time> 6/6 saving CSV files"
cat multilinguality-csv/part-* > $OUTPUT_DIR/multilinguality.csv
cat multilinguality-histogram/part-* > $OUTPUT_DIR/multilinguality-histogram.csv
cat multilinguality-histogram-raw/part-* > $OUTPUT_DIR/multilinguality-histogram-raw.csv
cat multilinguality-fieldIndex/part-* > $OUTPUT_DIR/multilinguality-fieldIndex.csv

if [[ ("$DO_KEEP" -eq 0) ]]; then
  # delete dirs
  rm -rf multilinguality-longform.parquet
  rm -rf multilinguality-statistics.parquet
  rm -rf multilinguality-median.parquet
  rm -rf multilinguality-histogram
  rm -rf multilinguality-histogram-raw
  rm -rf multilinguality-fieldIndex
  rm -rf multilinguality-csv
fi

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
