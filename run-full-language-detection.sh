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

SOURCE_DIR=/projects/pkiraly/data-export/${VERSION}/full
if [ ! -d ${SOURCE_DIR} ]; then
  echo "The source directory is not existing: ${SOURCE_DIR}"
  exit 1
fi
echo "source dir: ${SOURCE_DIR}"

OUTPUT_DIR=/projects/pkiraly/europeana-qa-data/${VERSION}
echo "output dir: ${OUTPUT_DIR}"

WEB_DATA_DIR=~/git/europeana-qa-webdata/${VERSION}
echo "web data dir: ${WEB_DATA_DIR}"

CSV=${VERSION}-language.csv
echo "csv: ${CSV}"
if [ -e ${CSV} ]; then
  rm ${CSV}
fi

exit

# (~ 4:56)
date +"%T"
LOG_FILE=run-all-language-detection.log
echo "Running language detection. Check log file: ${LOG_FILE}"
./run-all-language-detection --output-file ${CSV} --version ${VERSION} > ${LOG_FILE}

# Upload result file to HDFS (~ 0:16)
# cd ~/git/europeana-qa-spark
# hdfs dfs -put resultXX-language.csv /join


cd scala

# (~ 0:26)
date +"%T"
LOG_FILE=languages.log
echo "Running top level language measurement. Check log file: ${LOG_FILE}"
./languages.sh --input-file ../${CSV} --output-file ../output/languages.csv > ${LOG_FILE}

# (~ 0:46)
LOG_FILE=languages-per-collections.log
echo "Running Collection level language measurement. Check log file: ${LOG_FILE}"
./languages-per-collections.sh --input-file ../${CSV} --output-file ../output/languages-per-collections-groupped.txt > ${LOG_FILE} &

# Convert top level language results to JSON file

cd ../scripts
php languages-csv2json.php
cp ../output/languages.json $WEB_DATA_DIR

# Convert collection level language results to JSON files

# cd ~/git/europeana-qa-spark/scripts
php lang-group-to-json.php $VERSION

date +"%T"
echo "Languages count is done!"

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
