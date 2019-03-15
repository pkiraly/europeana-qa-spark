<?php

$dirs = file('dirs.txt');
$basedir = '/projects/pkiraly/europeana-qa-data/v2018-08/json';
$rows = [];
foreach ($dirs as $dir) {
  echo $dir;
  $dir = trim($dir);
  $values = process($dir);
  if (!empty($values)) {
    $rows[] = join(',', $values);
  }
}

file_put_contents('patterns.csv', join("\n", $rows));

function process($dir) {
  global $basedir;
  $file = $basedir . '/' . $dir . '/' . $dir . '-profiles-clustered.csv';
  if (!file_exists($file)) {
    return [];
  }
  $lines = file($basedir . '/' . $dir . '/' . $dir . '-profiles-clustered.csv');
  $patternCount = 0;
  $lastCluster = 0;
  foreach ($lines as $line) {
    $line = trim($line);
    if ($line != '' && !preg_match('/^#/', $line)) {
      $patternCount++;
      list($lastCluster, $profile, $nrOfFields, $occurence, $total, $percent) = explode(',', $line);
      # print_r($cells);
      # $lastCluster = $cells[0];
    }
  }
  return [$dir, $total, $patternCount, ($lastCluster + 1)];
}
