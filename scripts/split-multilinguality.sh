#!/usr/bin/env bash

VERSION=$1

if [[ ("$#" -ne 1) || ("$VERSION" == "") ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi

OUTPUT_DIR=/projects/pkiraly/europeana-qa-data/${VERSION}/json

if [ ! -d ${OUTPUT_DIR} ]; then
  mkdir -p ${OUTPUT_DIR}
fi

echo multilinguality
php split-spark-output.php --fileName ../output/multilinguality.csv --outputDir $OUTPUT_DIR --suffix multilinguality

echo multilinguality-histogram
php split-spark-output.php --fileName ../output/multilinguality-histogram.csv --outputDir $OUTPUT_DIR --suffix multilinguality-histogram

echo multilinguality-histogram-raw
php split-spark-output.php --fileName ../output/multilinguality-histogram-raw.csv --outputDir $OUTPUT_DIR --suffix multilinguality-histogram-raw

echo "split-multilinguality is ready"
