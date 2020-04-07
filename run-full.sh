#!/usr/bin/env bash

SECONDS=0
VERSION=$1

printf "%s %s> launch new abbreviation detection\n" $(date +"%F %T")
./run-full-abbreviation.sh $VERSION

printf "%s %s> launch completeness\n" $(date +"%F %T")
./run-full-completeness.sh $VERSION

printf "%s %s> launch multilinguality\n" $(date +"%F %T")
./run-full-multilingual-saturation.sh $VERSION

printf "%s %s> launch language detection\n" $(date +"%F %T")
./run-full-language-detection.sh --version $VERSION

printf "%s %s> launch CSV check\n" $(date +"%F %T")
./check-csvs.sh $VERSION

printf "%s %s> launch indexing\n" $(date +"%F %T")
./run-full-indexing.sh $VERSION

printf "%s %s> copy results to production\n" $(date +"%F %T")
cp -r ../europeana-qa-webdata/$VERSION /opt/data/europeana-qa-webdata/

cd ~/data-export/
printf "%s %s> count metadata and compress files (log: /home/pkiraly/data-export/count-and-compress-%s.log)\n" $(date +"%F %T") $VERSION
./count-and-compress.sh $VERSION > count-and-compress-$VERSION.log

printf "%s %s> archive compressed files (log: /home/pkiraly/data-export/save-full-download-%s.log)\n" $(date +"%F %T") $VERSION
./save-full.sh $VERSION > save-full-$VERSION.log

printf "%s %s> adjust the web configuration file /var/www/html/europeana-qa/config.cfg\n" $(date +"%F %T")
cp /var/www/html/europeana-qa/config.cfg /var/www/html/europeana-qa/config-pre-$VERSION.cfg
cat /var/www/html/europeana-qa/config.cfg \
  | sed "0,/version/{s/version/version[]=$VERSION\nversion/}" \
  | sed "0,/downloadable_version/{s/downloadable_version/downloadable_version[]=$VERSION\ndownloadable_version/}" \
  | sed "s/DEFAULT_VERSION=.*/DEFAULT_VERSION=$VERSION/" \
  > /var/www/html/europeana-qa/config-$VERSION.cfg
cp /var/www/html/europeana-qa/config-$VERSION.cfg /var/www/html/europeana-qa/config.cfg

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%s %s> run-full DONE\n" $(date +"%F %T")
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs

