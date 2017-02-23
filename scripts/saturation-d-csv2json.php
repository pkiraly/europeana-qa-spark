<?php

$file = '../scala/saturation-test-d.csv';
$collections = [];
$fh = fopen($file, "r");
while(!feof($fh)){
  $line = fgets($fh);
  if ($line == '') {
    continue;
  }
  $line = trim($line);
  $line = str_replace(['(', ')'], ['', ''], $line);
  list($collectionId, $field, $min, $max, $range, $mean, $median, $stdDeviation) = split(',', $line);
  $collections[$collectionId][$field] = [$min, $max, $range, $mean, $median, $stdDeviation];
}
fclose($fh);

foreach ($collections as $collectionId => $collection) {
  $json = [];
  foreach ($collection as $fieldName => $values) {
    $json[$fieldName] = [
      'min' => (double)$values[0], 
      'max' => (double)$values[1],
      'range' => (double)$values[2],
      'mean' => (double)$values[3],
      'median' => (double)$values[4],
      'std.dev' => (double)$values[5]
    ];
  }
  saveJson($collectionId, $json);
}

function saveJson($collectionId, $json) {
  $fileName = '../../europeana-qa-r/json2/' . $collectionId . '.saturation.json';
  file_put_contents($fileName, json_encode($json));
}