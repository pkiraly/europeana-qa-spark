#!/usr/bin/env bash

SECONDS=0

VERSION=$1
# VERSION=v2019-03
if [[ ("$#" -ne 1) || ("$VERSION" == "") ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi
echo "version: ${VERSION}"

SOURCE_DIR=/projects/pkiraly/data-export/${VERSION}/full
echo "source dir: ${SOURCE_DIR}"

CSV=${VERSION}-multilingual-saturation.csv
echo "csv: ${CSV}"

PARQUET=${VERSION}-multilingual-saturation.parquet
echo "parquet: ${PARQUET}"

date +"%T"
LOG_FILE=run-all-multilingual-saturation.log
echo "Running proxy based completeness. Check log file: ${LOG_FILE}"
./run-all-multilingual-saturation ${CSV} "" --extendedFieldExtraction ${VERSION} > ${LOG_FILE}
date +"%T"

echo "Collecting new abbreviation entries (if any)"
./extract-new-abbreviations.sh ${VERSION} ${LOG_FILE}

cd scala

date +"%T"
LOG_FILE=multilinguality-to-parquet.log
echo "create parquet file. Check log file: scala/${LOG_FILE}"
./multilinguality-to-parquet.sh ../${CSV} > ${LOG_FILE}

date +"%T"
LOG_FILE=multilinguality-all.log
echo "run completeness analysis. Check log file: scala/${LOG_FILE}"
./multilinguality-all.sh ../${PARQUET} --keep_dirs > ${LOG_FILE}

date +"%T"
cd ../scripts/
LOG_FILE=split-multilinguality.log
echo "split results. Check log file: scripts/${LOG_FILE}"
./split-multilinguality.sh ${VERSION} > ${LOG_FILE}

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

date +"%T"
echo "run-full-multilingual-saturation DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
