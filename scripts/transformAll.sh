#!/usr/bin/env bash

#
# Process all Scala output: transform to JSON, and deploy to the target directory
#
# * top level cardinality
# * top level frequency
# * top level language
# * collection level langugae
#

echo run cardinality
php cardinality-csv2json.php
cp cardinality.json ~/git/europeana-qa-r/json2

echo run frequency
php frequency-csv2json.php
cp frequency.json ~/git/europeana-qa-r/json2

echo run language top level
php frequency-csv2json.php
cp frequency.json ~/git/europeana-qa-r/json2

echo run language collection level
php lang-group-to-json.php

echo DONE
