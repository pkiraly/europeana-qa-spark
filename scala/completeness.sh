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

#  --executor-memory 6g \
CLASS=de.gwdg.europeanaqa.spark.completeness.CompletenessFromParquet
JAR=target/scala-2.11/europeana-qa_2.11-1.0.jar

spark-submit --driver-memory 3g --class $CLASS --master local[6] $JAR $INPUT $PHASE

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
