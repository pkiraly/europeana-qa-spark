# europeana-qa-spark
Spark interface

According to my measurements running the same code in Spark is 3-4 times faster than running it on Hadoop.
For running it on Spark, you should install Hadoop, Scala and finally Spark. The JSON files are stored in Hadoop Distributed File System at `/europeana` directory. In my case Hadoop's core-site.xml has `fs.default.name` property value: `hdfs://localhost:54310`, so I Spark access the files as `hdfs://localhost:54310/europeana/*.json`. The result will go to HDFS's `/result` directory. If that directory exists Spark (such as Hadoop) stops, so you should remove it first. The run on all Europeana records takes roughly two hours, so it is worth to run it at background and with nohup.

# Running
## multilinguality

### step 1. convert CSV to Parquet file (~40+ mins)
./multilinguality-to-parquet.sh [csv file]

### step 2. analyse multilinguality
./multilinguality-all.sh [parquet file] keep-dirs

e.g.

nohup ./multilinguality-all.sh ../v2018-08-multilingual-saturation.parquet keep-dirs > multilinguality-all.log &

### step 3. split result, store in final place
```
cd ../script
./split-multilinguality.sh
```

