#!/usr/bin/env bash

source base-dirs.sh

VERSION=$1
for TYPE in completeness multilingual-saturation languages
do
  echo $TYPE
  # ./split-spark-result.sh $TYPE $VERSION
done

DIR=${BASE_SOURCE_DIR}

source ../europeana-qa-solr/create-scripts.sh $VERSION $DIR
cd ../europeana-qa-solr
source ${VERSION}-index-all.sh

echo DONE

