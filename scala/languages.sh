hdfs dfs -rm -r /join/languages.csv
hdfs dfs -rm -r /join/languages-groupped.csv

spark-submit \
   --class Languages \
   --master local[*] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar \
   hdfs://localhost:54310/join/ result11-language.csv

hdfs dfs -getmerge /join/languages.csv languages.csv
hdfs dfs -getmerge /join/languages-groupped.txt languages-groupped.txt

rm .*.crc
