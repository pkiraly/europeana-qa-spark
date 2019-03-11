# multilinguality

## step 1. convert CSV to Parquet file (~40+ mins)
./multilinguality-to-parquet.sh [csv file]

## step 2. analyse multilinguality
./multilinguality-all.sh [parquet file] keep-dirs

e.g.

nohup ./multilinguality-all.sh ../v2018-08-multilingual-saturation.parquet keep-dirs > multilinguality-all.log &

## step 3. split result, store in final place
```
cd ../script
./split-multilinguality.sh
```
