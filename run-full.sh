#!/usr/bin/env bash

SECONDS=0
VERSION=$1

printf "%s %s> launch completeness\n" $(date +"%F %T")
./run-full-completeness.sh $VERSION

printf "%s %s> launch multilinguality\n" $(date +"%F %T")
./run-full-multilingual-saturation.sh $VERSION

printf "%s %s> launch language detection\n" $(date +"%F %T")
./run-full-language-detection.sh --version $VERSION

printf "%s %s> launch CSV check\n" $(date +"%F %T")
./check-csvs.sh $VERSION

printf "%s %s> launch indexing\n" $(date +"%F %T")
./run-full-indexing.sh $VERSION

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%s %s> run-full DONE\n" $(date +"%F %T")
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs

