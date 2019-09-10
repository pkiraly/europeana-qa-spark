#!/usr/bin/env bash

SECONDS=0

# VERSION=v2019-03

# set an initial value for the flag
VERSION=

# read the options
TEMP=`getopt -o v: --long version: -n 'test.sh' -- "$@"`
eval set -- "$TEMP"

# extract options and their arguments into variables.
while true ; do
    case "$1" in
        -v|--version)
            case "$2" in
                "") shift 2 ;;
                *) VERSION=$2 ; shift 2 ;;
            esac ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

if [[ "$VERSION" == "" ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi
echo "version: ${VERSION}"

source base-dirs.sh

export BASE_DIR=$(readlink -e .)
echo "base dir: $BASE_DIR"

SOURCE_DIR=$BASE_SOURCE_DIR/${VERSION}/full
if [ ! -d ${SOURCE_DIR} ]; then
  echo "The source directory is not existing: ${SOURCE_DIR}"
  exit 1
fi
echo "source dir: ${SOURCE_DIR}"

OUTPUT_DIR=$BASE_OUTPUT_DIR/${VERSION}
echo "output dir: ${OUTPUT_DIR}"

WEB_DATA_DIR=$BASE_WEB_DATA_DIR/${VERSION}
echo "web data dir: ${WEB_DATA_DIR}"

export LIMBO=$(readlink -e limbo)

if [[ ! -d output ]]; then
  mkdir output
fi

if [[ ! -d logs ]]; then
  mkdir logs
fi

LOG_DIR=$(readlink -e logs)
echo $LOG_DIR

CSV=$LIMBO/${VERSION}-language.csv
echo "csv: ${CSV}"
#if [ -e ${CSV} ]; then
#  rm ${CSV}
#fi

# (~ 4:56)
date +"%T"
LOG_FILE=${LOG_DIR}/run-all-language-detection.log
echo "Running language detection. Check log file: ${LOG_FILE}"
#scripts/record-processing/run-all-language-detection --output-file ${CSV} --version ${VERSION} --extendedFieldExtraction &> ${LOG_FILE}

# Upload result file to HDFS (~ 0:16)
# cd ~/git/europeana-qa-spark
# hdfs dfs -put resultXX-language.csv /join

#exit

# cd scala

# (~ 0:26)
#date +"%T"
#LOG_FILE=languages.log
#echo "Running top level language measurement. Check log file: ${LOG_FILE}"
#echo "scripts/analysis/languages.sh --input-file ${CSV} --output-file ../output/languages.csv &> ${LOG_FILE}"
#scripts/analysis/languages.sh --input-file ${CSV} --output-file ../output/languages.csv &> ${LOG_FILE}

#exit

# (~ 0:46)
#LOG_FILE=languages-per-collections.log
#echo "Running Collection level language measurement. Check log file: ${LOG_FILE}"
#scripts/analysis/languages-per-collections.sh --input-file ${CSV} --output-file ../output/languages-per-collections-groupped.txt > ${LOG_FILE} &


# Convert top level language results to JSON file

#cd ../scripts
#php languages-csv2json.php
#cp ../output/languages.json $WEB_DATA_DIR

# Convert collection level language results to JSON files

#php lang-group-to-json.php $VERSION


# Convert
#cd ../scala

LOG_FILE=${LOG_DIR}/languages-analysis.log
printf "$%s> Running language analysis. Check log file: %s\n" $(date +"%T") ${LOG_FILE}
scripts/analysis/languages-analysis.sh --input-file ${CSV} --output-file $BASE_DIR/output/languages-all.csv &> ${LOG_FILE}

# cd scripts
LOG_FILE=${LOG_DIR}/languages-to-json-and-split.log
printf "$%s> Running language to json and split. Check log file: %s\n" $(date +"%T") ${LOG_FILE}
php scripts/languages-all-to-json.php $BASE_DIR/output/languages-all.csv $WEB_DATA_DIR &> ${LOG_FILE}

date +"%T"
echo "Languages count is done!"

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
