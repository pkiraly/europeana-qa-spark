# europeana-qa-spark
Spark interface

According to my measurements running the same code in Spark is 3-4 times faster than running it on Hadoop.
For running it on Spark, you should install Hadoop, Scala and finally Spark. The JSON files are stored in Hadoop Distributed File System at `/europeana` directory. In my case Hadoop's core-site.xml has `fs.default.name` property value: `hdfs://localhost:54310`, so I Spark access the files as `hdfs://localhost:54310/europeana/*.json`. The result will go to HDFS's `/result` directory. If that directory exists Spark (such as Hadoop) stops, so you should remove it first. The run on all Europeana records takes roughly two hours, so it is worth to run it at background and with nohup.

    hdfs dfs -rm -r -skipTrash /result
    spark-submit --class com.nsdr.spark.CompletenessCount \
      --master local target/europeana-qa-spark-1.0-SNAPSHOT-jar-with-dependencies.jar \
      hdfs://localhost:54310/europeana/*.json \
      hdfs://localhost:54310/result \
      data-providers.txt \
      datasets.txt
    hdfs dfs -getmerge /result result.csv
