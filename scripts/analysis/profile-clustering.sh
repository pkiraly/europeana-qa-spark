#!/usr/bin/env bash
#
# Make profile clusters (ID.profile-patterns-clustered.csv)
#

# the input directory
WEB_DATA_DIR=$1

JAR_VERSION=0.7-SNAPSHOT
DIR=${WEB_DATA_DIR}/json/
JAR=target/europeana-qa-spark-${JAR_VERSION}-jar-with-dependencies.jar

for PREFIX_DIR in $DIR/*; do
  echo $PREFIX_DIR
  for SUBDIR in $PREFIX_DIR/*; do
    echo "  $SUBDIR"
    ID=$(echo "$SUBDIR" | sed -r 's,^.*/([^/]+)$,\1,')
    echo "$ID"

    java -Xmx2g -cp $JAR de.gwdg.metadataqa.api.similarity.ProfileReader \
      $SUBDIR/$ID.profile-field-counts.csv \
      $SUBDIR/$ID.profile-patterns.csv \
      > $SUBDIR/$ID.profile-patterns-clustered.csv

  done
done
