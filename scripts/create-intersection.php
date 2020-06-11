<?php

$OUTPUT_DIR = $argv[1];

$subdirs = ['cd', 'pd', 'cp', 'cdp'];

$intersections = ['c' => [], 'd' => [], 'p' => []];
foreach ($subdirs as $subdir) {
  $dir = $OUTPUT_DIR . '/json/' . $subdir;
  printf("processing dir: %s\n", $dir);
  if ($handle = opendir($dir)) {
    while (false !== ($entry = readdir($handle))) {
      if (preg_match('/^(cd|pd|cp|cdp)-(\d+)-(\d+)(-(\d+))?$/', $entry, $matches)) {
        echo "$entry\n";
        $file = $matches[0];
        $histogramFile = getHistogramFile($file);
        if (!file_exists($histogramFile)) {
          printf("%s is not existing.\n", $histogramFile);
          continue;
        }
        $count = readCount($histogramFile);
        $entry = (object)[
          'file' => $file,
          'count' => $count,
        ];
        if ($matches[1] == 'cd') {
          $c = $matches[2];
          $d = $matches[3];
          addEntry($entry, 'c', $c, 'd', $d);
          addEntry($entry, 'd', $d, 'c', $c);
        } else if ($matches[1] == 'pd') {
          $p = $matches[2];
          $d = $matches[3];
          addEntry($entry, 'p', $p, 'd', $d);
          addEntry($entry, 'd', $d, 'p', $p);
        } else if ($matches[1] == 'cp') {
          $c = $matches[2];
          $p = $matches[3];
          addEntry($entry, 'c', $c, 'p', $p);
          addEntry($entry, 'p', $p, 'c', $c);
        } else if ($matches[1] == 'cdp') {
          $c = $matches[2];
          $d = $matches[3];
          $p = $matches[5];
          addEntry($entry, 'c', $c, 'd', $d, 'p', $p);
          addEntry($entry, 'c', $c, 'p', $p, 'd', $d);
          addEntry($entry, 'd', $d, 'c', $c, 'p', $p);
          addEntry($entry, 'd', $d, 'p', $p, 'c', $c);
          addEntry($entry, 'p', $p, 'c', $c, 'd', $d);
          addEntry($entry, 'p', $p, 'd', $d, 'c', $c);
        }
        // print_r($matches);
      }
    }
    closedir($handle);
    // file_put_contents('intersections.json', json_encode($intersections));
  }
}
file_put_contents('proxy-based-intersections.json', json_encode($intersections));

function getHistogramFile($file) {
  global $dir;

  return sprintf('%s/%s/%s.completeness-histogram.csv', $dir, $file, $file);
}

function addEntry($entry, $k1, $v1, $k2, $v2, $k3 = NULL, $v3 = NULL) {
  global $intersections;

  if (!isset($intersections[$k1])) {
    $intersections[$k1] = [];
  }

  if (!isset($intersections[$k1][$v1])) {
    $intersections[$k1][$v1] = [];
  }

  if (!isset($intersections[$k1][$v1][$k2])) {
    $intersections[$k1][$v1][$k2] = [];
  }

  if (!isset($intersections[$k1][$v1][$k2][$v2])) {
    $intersections[$k1][$v1][$k2][$v2] = [];
  }

  if (is_null($k3)) {
    $intersections[$k1][$v1][$k2][$v2]['entry'] = $entry;
  } else {

    if (!isset($intersections[$k1][$v1][$k2][$v2][$k3])) {
      $intersections[$k1][$v1][$k2][$v2][$k3] = [];
    }

    if (!isset($intersections[$k1][$v1][$k2][$v2][$k3][$v3])) {
      $intersections[$k1][$v1][$k2][$v2][$k3][$v3]['entry'] = $entry;
    }
  }
}

function readCount($histogramFile) {
  global $dir;

  $lines = file($histogramFile);
  $count = 0;
  foreach ($lines as $line) {
    $line = trim($line);
    if (preg_match('/,PROVIDER_Proxy_rdf_about,/', $line)) {
      $parts = explode(',', $line);
      $bins = explode(';', $parts[2]);
      foreach ($bins as $bin) {
        list($range, $binCount) = explode(':', $bin);
        list($min, $max) = explode('-', $range);
        if ($min > 0 && $max > 0) {
          $count += $binCount;
        }
      }
      break;
    }
  }
  return $count;
}
