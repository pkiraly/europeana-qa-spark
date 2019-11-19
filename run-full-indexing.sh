#!/usr/bin/env bash

SECONDS=0
source base-dirs.sh

VERSION=$1
for TYPE in completeness multilingual-saturation language
do
  echo $TYPE
  ./split-spark-result.sh $TYPE $VERSION
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

