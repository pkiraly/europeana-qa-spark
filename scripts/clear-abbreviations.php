<?php

$WEB_DATA_DIR = $argv[1];

$sourceDir = '../../europeana-qa-api/src/main/resources/abbreviations';
$targetDir = $WEB_DATA_DIR;
$jsonDir = $WEB_DATA_DIR . '/json';

$types = [
  (object)['source' => 'datasets-v4.csv',       'target' => 'datasets.csv',       'prefix' => 'c'],
  (object)['source' => 'data-providers-v4.csv', 'target' => 'data-providers.csv', 'prefix' => 'd'],
  (object)['source' => 'providers-v2.csv',      'target' => 'providers.csv',      'prefix' => 'p-'],
  (object)['source' => 'countries-v2.csv',      'target' => 'countries.csv',      'prefix' => 'cn-'],
  (object)['source' => 'languages-v2.csv',      'target' => 'languages.csv',      'prefix' => 'l-'],
];

$start = microtime(TRUE);

foreach ($types as $type) {

  $inputFile = sprintf('%s/%s', $sourceDir, $type->source);
  $outputFile = sprintf('%s/%s', $targetDir, $type->target);
  printf("creating %s (%s)\n", $type->target, $outputFile);

  if (!file_exists($inputFile)) {
    printf("File doesn't exist: %s\n", $inputFile);
    continue;
  }

  if (file_exists($outputFile)) {
    unlink($outputFile);
  }

  $in = fopen($inputFile, "r");
  $entries = 0;
  while (($line = fgets($in)) != false) {
    if (strpos($line, ';') != false) {
      list($id, $name) = explode(';', $line, 2);
      $dir = sprintf('%s/%s%s', $jsonDir, $type->prefix, $id);
      printf("checking dir (%s)\n", $dir);
      if (file_exists($dir) && is_dir($dir)) {
        file_put_contents($outputFile, $line, FILE_APPEND);
        $entries++;
      }
    }
  }
  fclose($in);
  printf("... saved %d entries\n", $entries);
}

$duration = microtime(TRUE) - $start;
echo 'DONE in ', gmdate("H:i:s", (int)$duration), "\n";
