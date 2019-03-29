<?php

$inputFile = 'datasets-full.txt';
$outputFile = 'datasets-selected.txt';
$unusedListFile = 'unused-datasets.txt';

$start = microtime(TRUE);
if (file_exists($outputFile))
  unlink($outputFile);

// $ids_to_remove = split(",", file_get_contents($unusedListFile));

$in = fopen($inputFile, "r");
$ln = 1;
while (($line = fgets($in)) != false) {
  if (strpos($line, ';') != false) {
    list($id,$name) = explode(';', $line, 2);
    $dir = 'json/c' . $id;
    if (file_exists($dir) && is_dir($dir))
      file_put_contents($outputFile, $line, FILE_APPEND);
  }
}
fclose($in);

$duration = microtime(TRUE) - $start;
echo 'DONE in ', gmdate("H:i:s", (int)$duration), "\n";
