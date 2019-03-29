#!/usr/bin/env bash

SECONDS=0

VERSION=$1
if [[ ("$#" -ne 1) || ("$VERSION" == "") ]]; then
  echo "You should add an a version (such as 'v2018-08')!"
  exit 1
fi

OUTPUT_FILE=new-abbreviations-for-${VERSION}.txt
grep AbbreviationManager run-all-proxy-based-completeness.log \
  | sed -r 's/^.+ new entry: //' \
  | sed -r 's/^(.+) \(size: [0-9]+ -> [0-9]+\) abbreviations\/(.+)$/\2: \1/' \
  | sort \
  | uniq \
  > ${OUTPUT_FILE}

LINES=$(wc -l ${OUTPUT_FILE} | cut -f 1 -d ' ')
echo "${OUTPUT_FILE} created, containing ${LINES} abbreviations."
