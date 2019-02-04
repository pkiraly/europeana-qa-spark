<?php
$dir = '/projects/pkiraly/europeana-qa-data/v2018-08/json/';
if ($handle = opendir($dir)) {
  $intersections = ['c' => [], 'd' => [], 'p' => []];
  while (false !== ($entry = readdir($handle))) {
    if (preg_match('/^(cd|pd)-(\d+)-(\d+)$/', $entry, $matches)) {
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
        $intersections['c'][$c]['d'][$d] = $entry;
        $intersections['d'][$d]['c'][$c] = $entry;
      } else if ($matches[1] == 'pd') {
        $p = $matches[2];
        $d = $matches[3];
        $intersections['p'][$p]['d'][$d] = $entry;
        $intersections['d'][$d]['p'][$p] = $entry;
      }
      // print_r($matches);
    }
  }
  closedir($handle);
  file_put_contents('intersections.json', json_encode($intersections));
}

function getHistogramFile($file) {
  global $dir;

  return sprintf('%s/%s/%s.proxy-based-completeness-histogram.csv', $dir, $file, $file);
}

function readCount($histogramFile) {
  global $dir;

  $lines = file($histogramFile);
  $count = 0;
  foreach ($lines as $line) {
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
