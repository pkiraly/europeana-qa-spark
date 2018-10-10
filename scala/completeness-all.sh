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

if [[ ("$KEEP_DIR" == "keep_dir") ]]; then
  DO_KEEP=1
else
  DO_KEEP=0
fi

#if [[ ("$#" -ne 1) || ("$INPUT" == "") ]]; then
#  echo "You should add an input file!"
#  exit 1
#fi

#  --executor-memory 6g \
CLASS=de.gwdg.europeanaqa.spark.completeness.CompletenessFromParquet
JAR=target/scala-2.11/europeana-qa_2.11-1.0.jar

spark-submit --driver-memory 3g --class $CLASS --master local[6] $JAR $INPUT "prepare"
spark-submit --driver-memory 3g --class $CLASS --master local[6] $JAR $INPUT "statistics"
spark-submit --driver-memory 3g --class $CLASS --master local[6] $JAR $INPUT "median"
spark-submit --driver-memory 3g --class $CLASS --master local[6] $JAR $INPUT "histogram"
spark-submit --driver-memory 3g --class $CLASS --master local[6] $JAR $INPUT "join"

cat completeness-csv/part-* > ../output/completeness.csv
cat completeness-histogram/part-* > ../output/completeness-histogram.csv
cat completeness-fieldIndex/part-* > ../output/completeness-fieldIndex.csv

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
