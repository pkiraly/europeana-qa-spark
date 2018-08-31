<?php

$dir = '/projects/pkiraly/data-export/2018-08-24';

print_r($argv);

$hdfsFile = $argv[1];
echo $hdfsFile, "\n";

if (preg_match('/^(.*)\/([^\/]+)$/', $hdfsFile, $matches)) {
  // print_r($matches);
  $hdfsDir = $matches[1];
  $outputFile = $matches[2];
  echo $hdfsFile, ' --> ', $outputFile, "\n";

  $mergeCmd = sprintf("/usr/local/hadoop/bin/hdfs dfs -getmerge %s %s/%s.json", $hdfsFile, $dir, $outputFile);
  echo $mergeCmd, "\n";
  exec($mergeCmd, $mergeResult);
  // print_r($mergeResult);

  unlink(sprintf("%s/.%s.json.crc", $dir, $outputFile));

  $delCmd = sprintf("/usr/local/hadoop/bin/hdfs dfs -rm -r %s", $hdfsDir);
  echo $delCmd, "\n";
  $mergeResult = NULL;
  exec($delCmd, $mergeResult);
  print_r($mergeResult);
}
