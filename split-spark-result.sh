#!/usr/bin/env bash

TYPE=$1
VERSION=$2

source base-dirs.sh

FILE=limbo/${VERSION}-${TYPE}.csv
OUTPUT_DIR=${BASE_SOURCE_DIR}/${VERSION}/parts-${TYPE}
PREFIX=${OUTPUT_DIR}/part

if [ ! -d "$OUTPUT_DIR" ]; then
  mkdir $OUTPUT_DIR
fi

split -l 1000000 --verbose -d --additional-suffix .csv $FILE $PREFIX

