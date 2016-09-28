FILE=languages-per-collections-groupped.txt

hdfs dfs -rm -r /join/$FILE

spark-submit \
   --class LanguagesPerDataProviders \
   --master local[*] \
   target/scala-2.10/europeana-qa_2.10-1.0.jar \
   hdfs://localhost:54310/join/ result14-language.csv

hdfs dfs -getmerge /join/$FILE $FILE

rm .*.crc

echo DONE
