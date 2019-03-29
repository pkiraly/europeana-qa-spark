<?php

$VERSION = $argv[1];

$sourceDir = '~/git/europeana-qa-api/src/main/resources/abbreviations';
$targetDir = '~/git/europeana-qa-webdata/' . $VERSION;
$jsonDir = '/projects/pkiraly/europeana-qa-data/' . $VERSION . '/json';

$types = [
  (object)['source' => 'datasets-v3.txt',       'target' => 'datasets.txt',       'prefix' => 'c'],
  (object)['source' => 'data-providers-v3.txt', 'target' => 'data-providers.txt', 'prefix' => 'd'],
  (object)['source' => 'providers-v1.csv',      'target' => 'providers.csv',      'prefix' => 'p-'],
  (object)['source' => 'countries-v2.csv',      'target' => 'countries.csv',      'prefix' => 'cn-'],
  (object)['source' => 'languages-v1.csv',      'target' => 'languages.csv',      'prefix' => 'l-'],
];

$start = microtime(TRUE);

foreach ($types as $type) {
  printf("creating %s\n", $type->target);

  $inputFile = sprintf('%s/%s', $sourceDir, $type->source);
  $outputFile = sprintf('%s/%s', $targetDir, $type->target);

  if (!file_exists($inputFile)) {
    printf("File doesn't exist: %s\n", $inputFile);
    continue;
  }

  $in = fopen($inputFile, "r");
  $ln = 1;
  while (($line = fgets($in)) != false) {
    if (strpos($line, ';') != false) {
      list($id, $name) = explode(';', $line, 2);
      $dir = sprintf('%s/%s%s', $jsonDir, $type->prefix, $id);
      if (file_exists($dir) && is_dir($dir))
        file_put_contents($outputFile, $line, FILE_APPEND);
    }
  }
  fclose($in);
}

$duration = microtime(TRUE) - $start;
echo 'DONE in ', gmdate("H:i:s", (int)$duration), "\n";
