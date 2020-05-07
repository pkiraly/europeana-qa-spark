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
php split-spark-output.php --fileName ../output/profile-patterns.csv --outputDir $OUTPUT_DIR --suffix profile-patterns

echo completeness-histogram
php split-spark-output.php --fileName ../output/profile-field-counts.csv --outputDir $OUTPUT_DIR --suffix profile-field-counts

echo "split-profiles is ready"
