<?php
require_once('options.php');

$opts = make_options([
  'f:' => 'fileName:',
  'd:' => 'outputDir:',
  's:' => 'suffix:',
]);
$options = getopt($opts[0], $opts[1]);

printf("File name: %s\n", $options['fileName']);
printf("output dir: %s\n", $options['outputDir']);
printf("suffix: %s\n", $options['suffix']);

$start = microtime(TRUE);
$fileName = $options['fileName'];
if (!file_exists($fileName))
  die("The input file ($fileName) is not existing.\n");

$outputDir = $options['outputDir'];
if (!file_exists($outputDir))
  die("The output dir ($outputDir) is not existing.\n");

$start = microtime(TRUE);
$in = fopen($fileName, "r");
$out = [];
$ln = 1;
$prevId = '';
$lines = [];
while (($line = fgets($in)) != false) {
  if (strpos($line, ',') != false) {
    if ($ln++ % 10000 == 0) {
      echo number_format($ln, 0, '.', '.'), ' ';
    }
    $row = str_getcsv($line);
    $id = $row[0];
    if ($id != $prevId && $prevId != "") {
      saveContent($prevId, $lines);
      $lines = [];
    }
    $prevId = $id;
    $lines[] = $line;
  }
}
fclose($in);
saveContent($id, $lines);
echo "\n";

$duration = microtime(TRUE) - $start;
echo 'DONE in ', gmdate("H:i:s", (int)$duration), "\n";

function saveContent($id, $lines) {
  global $outputDir, $options;

  $parentDir = getParentDir($id);
  $dir = sprintf("%s/%s/%s", $outputDir, $parentDir, $id);
  if (!file_exists($dir)) {
    echo "Making directory: $dir\n";
    mkdir($dir);
  }
  $outputFile = sprintf("%s/%s.%s.csv", $dir, $id, $options['suffix']);
  file_put_contents($outputFile, implode('', $lines));
}

function getParentDir($id) {
  global $outputDir;

  $parentDir = '';
  if ($id == 'all') {
    $parentDir = 'a';
  } else {
    $parts = explode('-', $id);
    $parentDir = $parts[0];
  }

  $dir = sprintf("%s/%s", $outputDir, $parentDir);
  if (!file_exists($dir)) {
    echo "Making directory: $dir\n";
    mkdir($dir);
  }

  return $parentDir;
}
