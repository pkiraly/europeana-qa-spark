#!/usr/bin/env bash

SECONDS=0

# VERSION=v2019-03

# set an initial value for the flag
VERSION=
VERBOSE_MODE=0

# read the options
TEMP=`getopt -o v:b --long version:,verbose -n 'test.sh' -- "$@"`
eval set -- "$TEMP"
while true ; do
    case "$1" in
        -v|--version)
            case "$2" in
                "") shift 2 ;;
                *) VERSION=$2 ; shift 2 ;;
            esac ;;
        -b|--verbose) VERBOSE_MODE=1 ; shift ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

if [[ "$VERSION" == "" ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi
echo "# version: ${VERSION}"

source base-dirs.sh

export BASE_DIR=$(readlink -e .)
echo "# base dir: $BASE_DIR"

INPUT_DIR=$BASE_SOURCE_DIR/${VERSION}/full
if [ ! -d ${INPUT_DIR} ]; then
  echo "The input directory is not existing: ${INPUT_DIR}"
  exit 1
fi
echo "# input dir: ${INPUT_DIR}"

OUTPUT_DIR=$BASE_OUTPUT_DIR/${VERSION}
echo "# output dir: ${OUTPUT_DIR}"

WEB_DATA_DIR=$BASE_WEB_DATA_DIR/${VERSION}
echo "# web data dir: ${WEB_DATA_DIR}"

export LIMBO=$(readlink -e limbo)

if [[ ! -d output ]]; then
  mkdir output
fi

if [[ ! -d logs ]]; then
  mkdir logs
fi

LOG_DIR=$(readlink -e logs)
echo "# log dir: ${LOG_DIR}"

CSV=$LIMBO/${VERSION}-language.csv
echo "# csv: ${CSV}"

# ~03:39:46
function record_processing {
  if [ -e ${CSV} ]; then
    rm ${CSV}
  fi
  LOG_FILE=${LOG_DIR}/run-all-language-detection.log
  printf "%s %s> Running language detection. Check log file: %s\n" $(date +"%F %T") ${LOG_FILE}
  if [[ $VERBOSE_MODE -eq 1 ]]; then
    echo "scripts/record-processing/run-all-language-detection --output-file ${CSV} --version ${VERSION} --extended-field-extraction &> ${LOG_FILE}"
  fi
  scripts/record-processing/run-all-language-detection --output-file ${CSV} --version ${VERSION} --extended-field-extraction &> ${LOG_FILE}
}

# ~03:28:05
function analysis {
  LOG_FILE=${LOG_DIR}/languages-analysis.log
  printf "%s %s> Running language analysis. Check log file: %s\n" $(date +"%F %T") ${LOG_FILE}
  if [[ $VERBOSE_MODE -eq 1 ]]; then
    echo "scripts/analysis/languages-analysis.sh --input-file ${CSV} --output-file $BASE_DIR/output/languages-all.csv &> ${LOG_FILE}"
  fi
  scripts/analysis/languages-analysis.sh --input-file ${CSV} --output-file $BASE_DIR/output/languages-all.csv &> ${LOG_FILE}
}

# ~00:00:33
function split {
  LOG_FILE=${LOG_DIR}/languages-to-json-and-split.log
  printf "%s %s> Running language to json and split. Check log file: %s\n" $(date +"%F %T") ${LOG_FILE}
  if [[ $VERBOSE_MODE -eq 1 ]]; then
    echo "php scripts/languages-all-to-json.php $BASE_DIR/output/languages-all.csv $WEB_DATA_DIR &> ${LOG_FILE}"
  fi
  php scripts/languages-all-to-json.php $BASE_DIR/output/languages-all.csv $WEB_DATA_DIR &> ${LOG_FILE}
}

record_processing
analysis
split

printf "%s %s> Languages count is done!\n" $(date +"%F %T")

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs

