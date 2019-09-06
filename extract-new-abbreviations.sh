#!/usr/bin/env bash

SECONDS=0

VERSION=$1
LOG_FILE=$2

if [[ ("$#" -ne 2) || ("$VERSION" == "" || ("$LOG_FILE" == "")) ]]; then
  echo "You should add an a version (such as 'v2018-08') and a log file!"
  exit 1
fi

OUTPUT_FILE=new-abbreviations-for-${VERSION}.txt
grep AbbreviationManager ${LOG_FILE} \
  | sed -r 's/^.+ new entry: //' \
  | sed -r 's/^(.+) \(size: [0-9]+ -> [0-9]+\) abbreviations\/(.+)$/\2: \1/' \
  | sort \
  | uniq \
  > ${OUTPUT_FILE}

LINES=$(wc -l ${OUTPUT_FILE} | cut -f 1 -d ' ')
echo "${OUTPUT_FILE} created, containing ${LINES} abbreviations."
