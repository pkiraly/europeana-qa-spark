#!/usr/bin/env bash
#
# Run the top level aggregation of language features.
#
# Input
#   A file take place on HDFS /join directory (such as result*-language.csv)
#   This file should be the output of record-level language measurement
# Output
#   languages.csv

INPUT_FILE=
OUTPUT_FILE=languages.csv
USE_HDFS=0

TEMP=`getopt -o i:o:h --long input-file:,output-file:,use-hdfs -n 'test.sh' -- "$@"`
eval set -- "$TEMP"
while true ; do
    case "$1" in
        -i|--input-file)
            case "$2" in
                "") shift 2 ;;
                *) INPUT_FILE=$2 ; shift 2 ;;
            esac ;;
        -o|--output-file)
            case "$2" in
                "") shift 2 ;;
                *) OUTPUT_FILE=$2 ; shift 2 ;;
            esac ;;
        -h|--use-hdfs) USE_HDFS=1 ; shift ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

if [[ "${INPUT_FILE}" == "" ]]; then
  echo "You should add an input file!"
  exit 1
fi

if [[ "$SPARK_TEMP_DIR" = "" ]]; then
  source ../../base-dirs.sh
fi

if [[ "$BASE_DIR" != "" ]]; then
  SCALA_DIR=$BASE_DIR/scala
else
  current=$(dirname $0)
  echo "current: $current"
  SCALA_DIR=$(readlink -e $current/../../scala)
fi
echo "SCALA_DIR: $SCALA_DIR"

OUTPUT_DIR=${OUTPUT_FILE}-sparkdir
if [[ ${USE_HDFS} -eq 1 ]]; then
  hdfs dfs -rm -r /join/${OUTPUT_DIR}
fi

CLASS=de.gwdg.europeanaqa.spark.languages.LanguagesAll
JAR=$SCALA_DIR/target/scala-2.11/europeana-qa_2.11-1.0.jar
MEMORY=3g
CORES=6
CONF="spark.local.dir=$SPARK_TEMP_DIR"

SPARK_CORE_PARAMS="--driver-memory $MEMORY --executor-memory $MEMORY --master local[$CORES] --class $CLASS --conf $CONF"

spark-submit $SPARK_CORE_PARAMS $JAR $INPUT_FILE $OUTPUT_DIR "prepare"
# spark-submit $SPARK_CORE_PARAMS $JAR $INPUT_FILE $OUTPUT_DIR "statistics"

echo Retrieve $OUTPUT_FILE
if [[ ${USE_HDFS} -eq 1 ]]; then
  hdfs dfs -getmerge /join/$OUTPUT_DIR $OUTPUT_FILE
  rm .${OUTPUT_FILE}.crc
else
  echo ${OUTPUT_DIR}/part-* | xargs cat > ${OUTPUT_FILE}
fi

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
echo DONE
