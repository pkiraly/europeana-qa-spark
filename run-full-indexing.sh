#!/usr/bin/env bash

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

echo DONE

