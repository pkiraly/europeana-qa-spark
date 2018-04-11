#!/usr/bin/env bash

ANALYSIS=$1
if [ "$1" = "" ]; then
  ANALYSIS="completeness"
fi

VERSION=0.6-SNAPSHOT
JAR=~/.m2/repository/de/gwdg/metadataqa/europeana-qa-spark/$VERSION/europeana-qa-spark-$VERSION-jar-with-dependencies.jar
# JAR=target/europeana-qa-spark-0.4-SNAPSHOT-jar-with-dependencies.jar

java -cp $JAR de.gwdg.europeanaqa.spark.HeaderCommand --analysis $ANALYSIS
