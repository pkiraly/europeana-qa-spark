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
SECONDS=0

# INPUTFILE=result29-multilingual-saturation.csv.gz
# INPUTFILE=result29-multilingual-saturation.csv
INPUTFILE=result29-multilingual-saturation-light.csv
OUTPUTFILE=result29-multilingual-saturation-statistics
DIR=/home/pkiraly/git/europeana-qa-spark
HDFS_DIR=hdfs://localhost:54310/join

if hdfs dfs -test -d /join/${OUTPUTFILE}; then
  hdfs dfs -rm -r /join/$OUTPUTFILE
fi

if hdfs dfs -test -e /join/${OUTPUTFILE}-median; then
  hdfs dfs -rm -r /join/${OUTPUTFILE}-median
fi

if hdfs dfs -test -e /join/${OUTPUTFILE}-longform; then
  hdfs dfs -rm -r /join/${OUTPUTFILE}-longform
fi

spark-submit \
   --driver-memory 3g \
   --class SaturationWithHistogramForLight \
   --master local[6] \
   target/scala-2.11/europeana-qa_2.11-1.0.jar \
   $HDFS_DIR/$INPUTFILE \
   $HDFS_DIR/$OUTPUTFILE

echo Retrieve $OUTPUTFILE
hdfs dfs -getmerge /join/$OUTPUTFILE $OUTPUTFILE.csv
echo Retrieve ${OUTPUTFILE}-longform
hdfs dfs -getmerge /join/${OUTPUTFILE}-longform ${OUTPUTFILE}-longform.csv
echo Retrieve ${OUTPUTFILE}-median
hdfs dfs -getmerge /join/${OUTPUTFILE}-median ${OUTPUTFILE}-median.csv

rm .*.crc

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
