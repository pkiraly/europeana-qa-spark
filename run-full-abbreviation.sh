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

CSV=$LIMBO/${VERSION}-abbreviated-ids.csv
echo "# csv: ${CSV}"

PARQUET=$LIMBO/${VERSION}-abbreviation.parquet
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
LOG_FILE=${LOG_DIR}/abbreviations.log
echo "$time> Abbreviation check. Check log file: ${LOG_FILE}"
echo "$time> scripts/record-processing/run-all-abbreviation-check --output-file ${CSV} --extended-field-extraction --version ${VERSION} > ${LOG_FILE}"
scripts/record-processing/run-all-abbreviation-check \
  --output-file ${CSV} \
  --extended-field-extraction \
  --version ${VERSION} \
  --from-gz &> ${LOG_FILE}

time=$(date +"%F %T")
echo "$time> Collecting new abbreviation entries (if any)"
./extract-new-abbreviations.sh ${VERSION} ${LOG_FILE}

if [[ -s new-abbreviations-for-${VERSION}.txt ]]; then
  php update-abbreviations.php ${VERSION}
  cd ../europeana-qa-api
  git commit -am "Add abbreviation for ${VERSION}" && git push
  cd ../europeana-qa-spark
  ./build-after-abbreviations
fi

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

time=$(date +"%F %T")
echo "$time> run-full-abbreviation DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
