#!/usr/bin/env bash

DIR=/opt/data/europeana-qa-webdata/v2020-09/json/
JAR=target/europeana-qa-spark-0.7-SNAPSHOT-jar-with-dependencies.jar

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
