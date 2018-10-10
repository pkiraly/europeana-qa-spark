<?php
require_once('options.php');

$opts = make_options([
  'f:' => 'fileName:',
  'd:' => 'outputDir:',
  's:' => 'suffix:',
]);
$options = getopt($opts[0], $opts[1]);

$start = microtime(TRUE);
$fileName = $options['fileName'];
if (!file_exists($fileName))
  die("The input file ($fileName) is not existing.\n");

// '/projects/pkiraly/2018-03-23/split/multilinguality';
$outputDir = $options['outputDir'];
if (!file_exists($outputDir))
  die("The output dir ($outputDir) is not existing.\n");

$start = microtime(TRUE);
$in = fopen($fileName, "r");
$out = [];
// $intersections = ['c' => [], 'd' => []];
$ln = 1;
while (($line = fgets($in)) != false) {
  if (strpos($line, ',') != false) {
    if ($ln++ % 10000 == 0) {
      echo number_format($ln, 0, '.', '.'), ' ';
    }
    $row = str_getcsv($line);
    $id = $row[1];
    file_put_contents(sprintf("%s/%s/%s.%s.csv", $outputDir, $id, $id, $options['suffix']), $line, FILE_APPEND);
  }
}
fclose($in);

$duration = microtime(TRUE) - $start;
echo 'DONE in ', gmdate("H:i:s", (int)$duration), "\n";
