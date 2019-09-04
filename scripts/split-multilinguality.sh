#!/usr/bin/env bash

DIR=$1

if [[ ("$#" -ne 1) || ("$DIR" == "") ]]; then
  echo "You should add a full path!"
  exit 1
fi

OUTPUT_DIR=$DIR/json

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
