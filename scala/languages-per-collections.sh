#!/usr/bin/env bash
#
# Run the collection level aggregation of language features.
#
# Input
#   A file take place on HDFS /join directory (such as result*-language.csv)
#   This file should be the output of record-level language measurement
# Output
#   languages-per-collections-groupped.txt

INPUT=
OUTPUT_FILE=languages-per-collections-groupped.txt
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

OUTPUT_DIR=${OUTPUT_FILE}-sparkdir
if [[ ${USE_HDFS} -eq 1 ]]; then
  hdfs dfs -rm -r /join/${OUTPUT_DIR}
fi

CLASS=de.gwdg.europeanaqa.spark.languages.LanguagesPerDataProviders
JAR=target/scala-2.11/europeana-qa_2.11-1.0.jar
MEMORY=3g
CORES=6

spark-submit --driver-memory $MEMORY --executor-memory $MEMORY \
  --master local[$CORES] \
  --class $CLASS \
  $JAR \
  $INPUT_FILE $OUTPUT_DIR

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
