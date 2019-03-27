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

echo completeness
php split-spark-output.php --fileName ../output/completeness.csv --outputDir $OUTPUT_DIR --suffix completeness

echo completeness-histogram
php split-spark-output.php --fileName ../output/completeness-histogram.csv --outputDir $OUTPUT_DIR --suffix completeness-histogram

echo completeness-histogram-raw
php split-spark-output.php --fileName ../output/completeness-histogram-raw.csv --outputDir $OUTPUT_DIR --suffix completeness-histogram-raw

