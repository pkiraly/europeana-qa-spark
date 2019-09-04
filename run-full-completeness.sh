#!/usr/bin/env bash

SECONDS=0

VERSION=$1
# VERSION=v2019-03
if [[ ("$#" -ne 1) || ("$VERSION" == "") ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi
echo "version: ${VERSION}"

source base-dirs.sh
SOURCE_DIR=$BASE_SOURCE_DIR/${VERSION}/full
echo "source dir: ${SOURCE_DIR}"

OUTPUT_DIR=$BASE_OUTPUT_DIR/${VERSION}
echo "output dir: ${OUTPUT_DIR}"

WEB_DATA_DIR=$BASE_WEB_DATA_DIR/${VERSION}
echo "web data dir: ${WEB_DATA_DIR}"

CSV=${VERSION}-completeness.csv
echo "csv: ${CSV}"

if [ -e ${CSV} ]; then
  rm ${CSV}
fi

PARQUET=${VERSION}-completeness.parquet
echo "parquet: ${PARQUET}"

if [ -e ${PARQUET} ]; then
  rm -rf ${PARQUET}
fi

exit

date +"%T"
LOG_FILE=run-all-proxy-based-completeness.log
echo "Running proxy based completeness. Check log file: ${LOG_FILE}"
./run-all-proxy-based-completeness ${CSV} "" --extendedFieldExtraction ${VERSION} > ${LOG_FILE}

date +"%T"
echo "Collecting new abbreviation entries (if any)"
./extract-new-abbreviations.sh ${VERSION} ${LOG_FILE}

cd scala

date +"%T"
LOG_FILE=proxy-based-completeness-to-parquet.log
echo "create parquet file. Check log file: scala/${LOG_FILE}"
./proxy-based-completeness-to-parquet.sh ../${CSV} > ${LOG_FILE}

date +"%T"
LOG_FILE=proxy-based-completeness-all.log
echo "run completeness analysis. Check log file: scala/${LOG_FILE}"
./proxy-based-completeness-all.sh ../${PARQUET} keep_dirs > ${LOG_FILE}

date +"%T"
cd ../scripts/
LOG_FILE=split-completeness.log
echo "split results. Check log file: scripts/${LOG_FILE}"
./split-completeness.sh ${VERSION}

date +"%T"
LOG_FILE=create-intersection.log
echo "create intersection. Check log file: scripts/${LOG_FILE}"
php create-intersection.php ${OUTPUT_DIR} > ${LOG_FILE}

if [ ! -d ${WEB_DATA_DIR} ]; then
  mkdir -p ${WEB_DATA_DIR}
  ln -s ${OUTPUT_DIR}/json ${WEB_DATA_DIR}/json
fi
cp proxy-based-intersections.json ${WEB_DATA_DIR}

date +"%T"
LOG_FILE=clear-abbreviations.log
echo "Clear abbreviations. Check log file: scripts/${LOG_FILE}"
php clear-abbreviations.php ${WEB_DATA_DIR} ${OUTPUT_DIR} > ${LOG_FILE}

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

date +"%T"
echo "run-full-completeness DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
