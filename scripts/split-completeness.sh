#!/usr/bin/env bash

DIR=$1

if [[ ("$#" -ne 1) || ("$DIR" == "") ]]; then
  echo "You should add full path!"
  exit 1
fi

OUTPUT_DIR=$DIR/json

if [ ! -d ${OUTPUT_DIR} ]; then
  mkdir -p ${OUTPUT_DIR}
fi

echo completeness
php split-spark-output.php --fileName ../output/completeness.csv \
    --outputDir $OUTPUT_DIR \
    --suffix completeness

echo completeness-histogram
php split-spark-output.php --fileName ../output/completeness-histogram.csv \
    --outputDir $OUTPUT_DIR \
    --suffix completeness-histogram

echo completeness-histogram-raw
php split-spark-output.php --fileName ../output/completeness-histogram-raw.csv \
    --outputDir $OUTPUT_DIR \
    --suffix completeness-histogram-raw

echo "split-completeness is ready"
