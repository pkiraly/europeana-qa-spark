#!/usr/bin/env bash

VERSION=$1
SECONDS=0

if [[ ! -d temp ]]; then
  mkdir temp
fi

for TYPE in completeness language multilingual-saturation; do
  echo $TYPE
  FILE="limbo/${VERSION}-${TYPE}.csv"
  awk -F, '{print $1}' $FILE > t
  sort -T ./temp t > t-sorted
  uniq -c t-sorted > t-uniq
  count=$(grep -c -v " 1 " t-uniq)
  printf "There are %d non uniqe identifiers in %s" $count $FILE
done

rm -rf temp t t-sorted

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%s %s> check-csvs DONE\n" $(date +"%F %T")
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs

