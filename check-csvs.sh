#!/usr/bin/env bash

VERSION=$1

if [[ ! -d temp ]]; then
  mkdir temp
fi

for TYPE in completeness language multilingual-saturation; do
  echo $TYPE
  awk -F, '{print $1}' limbo/$VERSION-$TYPE.csv > t
  sort -T ./temp t > t-sorted
  uniq -c t-sorted > t-uniq
  grep -c -v " 1 " t-uniq
done

rm -rf temp t t-sorted
