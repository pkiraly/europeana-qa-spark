#!/usr/bin/env bash

JAR=~/.m2/repository/de/gwdg/metadataqa/europeana-qa-spark/0.5-SNAPSHOT/europeana-qa-spark-0.5-SNAPSHOT-jar-with-dependencies.jar
# JAR=target/europeana-qa-spark-0.4-SNAPSHOT-jar-with-dependencies.jar

java -cp $JAR de.gwdg.europeanaqa.spark.HeaderCommand
