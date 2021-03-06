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

echo split profile-patterns
php split-spark-output.php --fileName ../output/profiles-patterns.csv --outputDir $OUTPUT_DIR --suffix profile-patterns

echo split profile-field-counts
php split-spark-output.php --fileName ../output/profiles-field-counts.csv --outputDir $OUTPUT_DIR --suffix profile-field-counts

echo "split-profiles is ready"
