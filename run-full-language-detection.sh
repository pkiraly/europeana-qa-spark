#!/usr/bin/env bash

SECONDS=0

# VERSION=v2019-03

# set an initial value for the flag
VERSION=

# read the options
TEMP=`getopt -o v:b --long version:,verbose -n 'test.sh' -- "$@"`
eval set -- "$TEMP"

VERBOSE_MODE=0
# extract options and their arguments into variables.
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
LOG_FILE=${LOG_DIR}/run-all-language-detection.log
printf "%s> Running language detection. Check log file: %s\n" $(date +"%T") ${LOG_FILE}
if [[ $VERBOSE_MODE -eq 1 ]]; then
  echo "scripts/record-processing/run-all-language-detection --output-file ${CSV} --version ${VERSION} --extendedFieldExtraction &> ${LOG_FILE}"
fi
#scripts/record-processing/run-all-language-detection --output-file ${CSV} --version ${VERSION} --extendedFieldExtraction &> ${LOG_FILE}

LOG_FILE=${LOG_DIR}/languages-analysis.log
printf "%s> Running language analysis. Check log file: %s\n" $(date +"%T") ${LOG_FILE}
if [[ $VERBOSE_MODE -eq 1 ]]; then
  echo "scripts/analysis/languages-analysis.sh --input-file ${CSV} --output-file $BASE_DIR/output/languages-all.csv &> ${LOG_FILE}"
fi
scripts/analysis/languages-analysis.sh --input-file ${CSV} --output-file $BASE_DIR/output/languages-all.csv &> ${LOG_FILE}

# cd scripts
LOG_FILE=${LOG_DIR}/languages-to-json-and-split.log
printf "%s> Running language to json and split. Check log file: %s\n" $(date +"%T") ${LOG_FILE}
if [[ $VERBOSE_MODE -eq 1 ]]; then
  echo "php scripts/languages-all-to-json.php $BASE_DIR/output/languages-all.csv $WEB_DATA_DIR &> ${LOG_FILE}"
fi
php scripts/languages-all-to-json.php $BASE_DIR/output/languages-all.csv $WEB_DATA_DIR &> ${LOG_FILE}

printf "%s> Languages count is done!\n" $(date +"%T")

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
