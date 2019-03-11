#!/bin/bash

OUTPUT_DIR=/projects/pkiraly/europeana-qa-data/v2018-08/json

echo completeness
php split-spark-output.php --fileName ../output/completeness.csv --outputDir $OUTPUT_DIR --suffix completeness

echo completeness-histogram
php split-spark-output.php --fileName ../output/completeness-histogram.csv --outputDir $OUTPUT_DIR --suffix completeness-histogram

echo completeness-histogram-raw
php split-spark-output.php --fileName ../output/completeness-histogram-raw.csv --outputDir $OUTPUT_DIR --suffix completeness-histogram-raw

