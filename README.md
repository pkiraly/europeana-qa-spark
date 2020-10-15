# europeana-qa-spark
Spark interface

According to my measurements running the same code in Spark is 3-4 times faster than running it on Hadoop.
For running it on Spark, you should install Hadoop, Scala and finally Spark. The JSON files are stored in Hadoop Distributed File System at `/europeana` directory. In my case Hadoop's core-site.xml has `fs.default.name` property value: `hdfs://localhost:54310`, so I Spark access the files as `hdfs://localhost:54310/europeana/*.json`. The result will go to HDFS's `/result` directory. If that directory exists Spark (such as Hadoop) stops, so you should remove it first. The run on all Europeana records takes roughly two hours, so it is worth to run it at background and with nohup.

# Running

```
nohup ./run-full.sh v2020-07 > logs/run-full-v2020-07.log &
```

## Completeness

### step 1. extract features from the Europeana JSON dump (~6:40 hours)
```
./run-all-proxy-based-completeness [output CSV] [--skipEnrichments] [--extendedFieldExtraction]
```

e.g.

```
nohup ./run-all-proxy-based-completeness v2018-08-completeness.csv "" --extendedFieldExtraction \
  > run-all-proxy-based-completeness.log &
```

### step 2. convert CSV to Parquet file (~40+ mins)
```
./proxy-based-completeness-to-parquet.sh [csv file]
```

### step 3. analyse completeness (~9 hours)
```
./proxy-based-completeness-all [parquet file] --keep-dirs
```

e.g.

```
nohup ./proxy-based-completeness-all.sh v2018-08-completeness2.parquet keep_dirs \
  > proxy-based-completeness-all.log &
```

It will produce three files:

* [project]/output/completeness.csv
* [project]/output/completeness-histogram.csv
* [project]/output/completeness-histogram-raw.csv

### step 4. split result, store in final place (~18 mins)
```
cd ../scripts
./split-completeness.sh $VERSION
```

## multilinguality

### step 1. extract features from the Europeana JSON dump (~7 hours)
```
./run-all-multilingual-saturation [output CSV] "" --extendedFieldExtraction
```

e.g.

```
nohup ./run-all-multilingual-saturation v2018-08-multilingual-saturation.csv "" --extendedFieldExtraction \
  > multilingual-saturation.log &
```

### step 2. convert CSV to Parquet file (~40+ mins)
```
./multilinguality-to-parquet.sh [csv file]
```

### step 3. analyse multilinguality (~9 hours)
```
./multilinguality-all.sh [parquet file] --keep-dirs
```

e.g.

```
nohup ./multilinguality-all.sh ../v2018-08-multilingual-saturation.parquet --keep-dirs \
  > multilinguality-all.log &
```

### step 4. split result, store in final place (~18 mins)
```
cd ../script
./split-multilinguality.sh
```

## Record patterns

## Uniqueness
