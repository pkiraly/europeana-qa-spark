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

CSV=${VERSION}-completeness.csv
echo "csv: ${CSV}"

PARQUET=${VERSION}-completeness.parquet
echo "parquet: ${PARQUET}"

LOG_FILE=run-all-proxy-based-completeness.log
echo "Running proxy based completeness. Check log file: ${LOG_FILE}"
echo "./run-all-proxy-based-completeness ${CSV} '' --extendedFieldExtraction ${VERSION} > ${LOG_FILE}"
./run-all-proxy-based-completeness ${CSV} "" --extendedFieldExtraction ${VERSION} > ${LOG_FILE}

echo "Collecting new abbreviation entries (if any)"
grep AbbreviationManager ${LOG_FILE} | sed -r 's/^.+ new entry: //' | sort | uniq > new-abbreviations-for-${VERSION}.txt

cd scala

echo "create parquet file"
./proxy-based-completeness-to-parquet.sh ../${CSV}

LOG_FILE=proxy-based-completeness-all.log
echo "run completeness analysis. Check log file: scala/${LOG_FILE}"
./proxy-based-completeness-all.sh ../${PARQUET} keep_dirs > proxy-based-completeness-all.log

cd ../scripts/
echo "split results."
./split-completeness.sh ${VERSION}

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
