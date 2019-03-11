#!/bin/bash

OUTPUT_DIR=/projects/pkiraly/europeana-qa-data/v2018-08/json

echo multilinguality
php split-spark-output.php --fileName ../output/multilinguality.csv --outputDir $OUTPUT_DIR --suffix multilinguality

echo multilinguality-histogram
php split-spark-output.php --fileName ../output/multilinguality-histogram.csv --outputDir $OUTPUT_DIR --suffix multilinguality-histogram

echo multilinguality-histogram-raw
php split-spark-output.php --fileName ../output/multilinguality-histogram-raw.csv --outputDir $OUTPUT_DIR --suffix multilinguality-histogram-raw

