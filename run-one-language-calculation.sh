#!/usr/bin/env sh

FILE=$1
HDFS=hdfs://localhost:54310
IN=$HDFS/europeana/$FILE
RES=$HDFS/result
OUT=$(echo $FILE | sed 's,.json,.txt,')
CRC=.$OUT.crc

echo "processing language count for $FILE ..."

if hdfs dfs -test -d /result; then
  hdfs dfs -rm -r -f -skipTrash /result
fi

spark-submit --class com.nsdr.spark.LanguageCount --master local[*] \
  target/europeana-qa-spark-1.0-SNAPSHOT-jar-with-dependencies.jar \
  $IN $RES \
  data-providers.txt \
  datasets.txt

hdfs dfs -getmerge /result $OUT
rm $CRC
hdfs dfs -rm -r -f -skipTrash /result

