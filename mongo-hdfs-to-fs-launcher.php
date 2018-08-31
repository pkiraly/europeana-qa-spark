<?php
/**
 * before start:
 *   ./prepare.sh saturation
 *
 * launch:
 *   crontab -e
 *   *\/ 1 * * * * cd ~/git/europeana-qa-r && php saturation-launcher.php >> launch-report.log
 *
 * monitoring:
 *   watch './running-status.sh saturation'
 */

define('MAX_THREADS', 2);
// echo 'hello', "\n";

// echo 'who am i? ', exec('whoami'), "\n";
// echo 'SHELL: ', exec('echo $SHELL'), "\n";
// echo 'PATH: ', exec('echo $PATH'), "\n";

$running_self = exec('ps aux | grep "[ ]mongo-hdfs-to-fs-launcher.php" | grep -v europeana-qa-spark | wc -l');
if ($running_self != 1)
  exit;

$hdfsFiles = getFileList();
$processedFiles = [];
print_r($hdfsFiles);

$PHPfile = 'single-mongo-export.php';
$endTime = time() + 300;
$i = 1;
while (time() < $endTime) {
  $threads = exec('ps aux | grep "[ ]' . $PHPfile . '" | wc -l');
  # echo 'threads: ', $threads, "\n";
  if ($threads < MAX_THREADS) {
    if (count($hdfsFiles) == 0) {
      $hdfsFiles = getFileList();
      echo "recheck files in HDFS\n";
      print_r($hdfsFiles);
    }
    if (count($hdfsFiles) > 0) {
      $hdfsFile = array_shift($hdfsFiles);
      if (!empty($hdfsFile)) {
        if (!in_array($hdfsFile, $processedFiles)) {
          $processedFiles[] = $hdfsFile;
          echo 'hdfsFile: ', $hdfsFile, "\n";
          list($usec, $sec) = explode(" ", microtime());
          $usec = str_replace('0.', '', $usec);
          $time = date("H:i:s", $sec) . '.' . $usec;
          $cmd = sprintf("nohup php %s %s >> single-mongo-export.log &", $PHPfile, $hdfsFile);
          echo $time, ' cmd: ', $cmd, "\n";
          exec($cmd);
        }
      }
    }
  }
  sleep(1);
}

do {
  $threads = exec('ps aux | grep "[ ]' . $PHPfile . '" | wc -l');
  if ($threads > 0) {
    echo '.';
    sleep(1);
  }
} while ($threads > 0);

echo "DONE\n";

function getFileList() {
  $hdfsFiles = NULL;
  // exec('hdfs dfs -ls -R /mongo-result2/', $hdfsFilesX);
  // echo 'X', "\n";
  // print_r($hdfsFilesX);
  exec('/usr/local/hadoop/bin/hdfs dfs -ls -R /mongo-result2/ | grep "/task_" | grep "/part-" | awk "{print \$8}"', $hdfsFiles);
  return $hdfsFiles;
}
