hdfs dfs -rm -r /join/cardinality.csv
hdfs dfs -rm -r /join/frequency.csv

spark-submit \
   --class Cardinality \
   --master local[*] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar \
   hdfs://localhost:54310/join/result11.csv \
   hdfs://localhost:54310/join/cardinality.csv \
   hdfs://localhost:54310/join/frequency.csv

hdfs dfs -getmerge /join/cardinality.csv cardinality.csv
hdfs dfs -getmerge /join/frequancy.csv frequency.csv
rm .*.crc