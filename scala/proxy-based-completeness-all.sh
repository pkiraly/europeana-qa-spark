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

echo "do keep? " $DO_KEEP
source ../base-dirs.sh

#if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
#  echo "You should add an input file!"
#  exit 1
#fi

INPUT_PATH=$(readlink -e $INPUT)
INPUT_DIR=$(dirname $INPUT_PATH)
echo "INPUT_DIR: $INPUT_DIR"

#  --executor-memory 6g \
CLASS=de.gwdg.europeanaqa.spark.completeness.ProxyBasedCompletenessFromParquet
JAR=target/scala-2.11/europeana-qa_2.11-1.0.jar
MEMORY=3g
CORES=6
CONF="spark.local.dir=$SPARK_TEMP_DIR"

COMMON_PARAMS="--driver-memory $MEMORY --class $CLASS --master local[$CORES] --conf $CONF $JAR $INPUT"
echo $COMMON_PARAMS

time=$(date +"%T")
echo "$time> prepare phase (../logs/completeness-analysis-prepare.log)"
spark-submit $COMMON_PARAMS "prepare" &> ../logs/completeness-analysis-prepare.log

time=$(date +"%T")
echo "$time> statistics phase (../logs/completeness-analysis-statistics.log)"
spark-submit $COMMON_PARAMS "statistics" &> ../logs/completeness-analysis-statistics.log

time=$(date +"%T")
echo "$time> median phase (../logs/completeness-analysis-median.log)"
spark-submit $COMMON_PARAMS "median" &> ../logs/completeness-analysis-median.log

time=$(date +"%T")
echo "$time> histogram phase (../logs/completeness-analysis-histogram.log)"
spark-submit $COMMON_PARAMS "histogram" &> ../logs/completeness-analysis-histogram.log

time=$(date +"%T")
echo "$time> join phase (../logs/completeness-analysis-join.log)"
spark-submit $COMMON_PARAMS "join" &> ../logs/completeness-analysis-join.log

time=$(date +"%T")
echo "$time> save files"
cat $INPUT_DIR/completeness-csv/part-* > ../output/completeness.csv
cat $INPUT_DIR/completeness-histogram/part-* > ../output/completeness-histogram.csv
cat $INPUT_DIR/completeness-histogram-raw/part-* > ../output/completeness-histogram-raw.csv
cat $INPUT_DIR/completeness-fieldIndex/part-* > ../output/completeness-fieldIndex.csv

if [[ ("$DO_KEEP" -eq 0) ]]; then
  # delete dirs
  rm -rf completeness-longform.parquet
  rm -rf completeness-statistics.parquet
  rm -rf completeness-median.parquet
  rm -rf completeness-histogram
  rm -rf completeness-fieldIndex
  rm -rf completeness-csv
fi

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
