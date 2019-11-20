#!/usr/bin/env bash

SECONDS=0
VERSION=$1
source base-dirs.sh

SPLIT_LOG="logs/split-spark-result.log"
if [[ -e $SPLIT_LOG ]]; then
  rm $SPLIT_LOG
fi

for TYPE in completeness multilingual-saturation language
do
  printf "%s %s> splitting %s (logs: %s)\n" $(date +"%F %T") $TYPE $SPLIT_LOG
  ./split-spark-result.sh $TYPE $VERSION >> $SPLIT_LOG
done

echo "Split done"

# DIR=${BASE_SOURCE_DIR}
# source ../europeana-qa-solr/create-scripts.sh $VERSION $DIR
cd ../europeana-qa-solr
# source ${VERSION}-index-all.sh
source index-all.sh ${VERSION}

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%s %s> run-full-indexing DONE\n" $(date +"%F %T")
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs

