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

if [[ ("$KEEP_DIRS" == "keep_dirs") ]]; then
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
CLASS=de.gwdg.europeanaqa.spark.profiles.ProfilesForAllIds
JAR=$SCALA_DIR/target/scala-2.11/europeana-qa_2.11-1.0.jar
MEMORY=3g
CORES=6
CONF="spark.local.dir=$SPARK_TEMP_DIR"

COMMON_PARAMS="--driver-memory $MEMORY --class $CLASS --master local[$CORES] --conf $CONF $JAR $INPUT"
echo $COMMON_PARAMS

time=$(date +"%T")
echo "$time> prepare phase ($LOG_DIR/profile-analysis-prepare.log)"
spark-submit $COMMON_PARAMS "prepare" &> $LOG_DIR/profile-analysis-prepare.log

time=$(date +"%T")
echo "$time> statistics phase ($LOG_DIR/profile-analysis-statistics.log)"
spark-submit $COMMON_PARAMS "statistics" &> $LOG_DIR/profile-analysis-statistics.log

time=$(date +"%T")
echo "$time> save files"
cat $INPUT_DIR/profiles-patterns.csv.dir/part-* > $OUTPUT_DIR/profiles-patterns.csv
cat $INPUT_DIR/profiles-field-counts.csv.dir/part-* > $OUTPUT_DIR/profiles-field-counts.csv
cat $INPUT_DIR/profiles-field-index.csv.dir/part-* > $OUTPUT_DIR/profiles-field-index.csv

if [[ ("$DO_KEEP" -eq 0) ]]; then
  # delete dirs
  rm -rf profiles-patterns.csv.dir
  rm -rf profiles-field-counts.csv.dir
  rm -rf profiles-field-index.csv.dir
  rm -rf profiles-longform.parquet
fi

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
