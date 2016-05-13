CSV_FILE=frequency.csv
hdfs dfs -rm -r /join/$CSV_FILE

spark-submit \
   --class Frequency \
   --master local[*] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar \
   hdfs://localhost:54310/join/result11.csv \
   hdfs://localhost:54310/join/$CSV_FILE

hdfs dfs -getmerge /join/$CSV_FILE $CSV_FILE
rm .*.crc
