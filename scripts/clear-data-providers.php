<?php

$inputFile = 'data-providers-full.txt';
$outputFile = 'data-providers-selected.txt';

$start = microtime(TRUE);
if (file_exists($outputFile))
  unlink($outputFile);

$in = fopen($inputFile, "r");
$ln = 1;
while (($line = fgets($in)) != false) {
  if (strpos($line, ';') != false) {
    list($id,$name) = explode(';', $line, 2);
    $dir = 'json/d' . $id;
    if (file_exists($dir) && is_dir($dir))
      file_put_contents($outputFile, $line, FILE_APPEND);
  }
}
fclose($in);

$duration = microtime(TRUE) - $start;
echo 'DONE in ', gmdate("H:i:s", (int)$duration), "\n";
