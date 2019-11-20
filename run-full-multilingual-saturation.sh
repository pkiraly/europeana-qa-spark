#!/usr/bin/env bash

SECONDS=0

VERSION=$1
# VERSION=v2019-03
if [[ ("$#" -ne 1) || ("$VERSION" == "") ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi
echo "# version: ${VERSION}"

export BASE_DIR=$(readlink -e .)
echo "# base dir: $BASE_DIR"

source base-dirs.sh
SOURCE_DIR=$BASE_SOURCE_DIR/${VERSION}/full
echo "# source dir: ${SOURCE_DIR}"

OUTPUT_DIR=$BASE_OUTPUT_DIR/${VERSION}
echo "# output dir: ${OUTPUT_DIR}"

WEB_DATA_DIR=$BASE_WEB_DATA_DIR/${VERSION}
echo "# web data dir: ${WEB_DATA_DIR}"

if [[ ! -d limbo ]]; then
  mkdir limbo
fi

LIMBO=$(readlink -e limbo)

CSV=$LIMBO/${VERSION}-multilingual-saturation.csv
echo "# csv: ${CSV}"

PARQUET=$LIMBO/${VERSION}-multilingual-saturation.parquet
echo "# parquet: ${PARQUET}"

if [[ ! -d output ]]; then
  mkdir output
fi

if [[ ! -d logs ]]; then
  mkdir logs
fi

LOG_DIR=$(readlink -e logs)
echo "# log dir: ${LOG_DIR}"

if [ -e ${CSV} ]; then
  rm ${CSV}
fi

time=$(date +"%F %T")
LOG_FILE=${LOG_DIR}/multilinguality-record-processing.log
echo "$time> Running multilingual saturation by records. Check log file: ${LOG_FILE}"
echo "scripts/record-processing/run-all-multilingual-saturation  --output-file ${CSV} --extended-field-extraction --version ${VERSION} &> ${LOG_FILE}"
scripts/record-processing/run-all-multilingual-saturation  --output-file ${CSV} --extended-field-extraction --version ${VERSION} &> ${LOG_FILE}

time=$(date +"%F %T")
echo "$time> Collecting new abbreviation entries (if any)"
./extract-new-abbreviations.sh ${VERSION} ${LOG_FILE}

if [ -e ${PARQUET} ]; then
  rm -rf ${PARQUET}
fi

time=$(date +"%F %T")
LOG_FILE=${LOG_DIR}/multilinguality-to-parquet.log
echo "$time> create parquet file. Check log file: ${LOG_FILE}"
scripts/analysis/multilinguality-to-parquet.sh ${CSV} &> ${LOG_FILE}

time=$(date +"%F %T")
LOG_FILE=${LOG_DIR}/multilinguality-analysis.log
echo "$time> run multilingual saturation analysis by collections. Check log file: ${LOG_FILE}"
scripts/analysis/multilinguality-all.sh ${PARQUET} --keep_dirs &> ${LOG_FILE}

cd scripts/
time=$(date +"%F %T")
LOG_FILE=${LOG_DIR}/multilinguality-split.log
echo "$time> split results. Check log file: ${LOG_FILE}"
./split-multilinguality.sh ${WEB_DATA_DIR} &> ${LOG_FILE}

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

time=$(date +"%F %T")
echo "$time> run-full-multilingual-saturation DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
