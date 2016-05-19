<?php

$order = [
  'aggregated', 'proxy_dc_title', 'proxy_dcterms_alternative', 'proxy_dc_description',
  'proxy_dc_creator', 'proxy_dc_publisher', 'proxy_dc_contributor', 'proxy_dc_type',
  'proxy_dc_identifier', 'proxy_dc_language', 'proxy_dc_coverage', 'proxy_dcterms_temporal',
  'proxy_dcterms_spatial', 'proxy_dc_subject', 'proxy_dc_date', 'proxy_dcterms_created',
  'proxy_dcterms_issued', 'proxy_dcterms_extent', 'proxy_dcterms_medium',
  'proxy_dcterms_provenance', 'proxy_dcterms_hasPart', 'proxy_dcterms_isPartOf',
  'proxy_dc_format', 'proxy_dc_source', 'proxy_dc_rights', 'proxy_dc_relation',
  'aggregation_edm_rights', 'aggregation_edm_provider', 'aggregation_edm_dataProvider',
];

$codes = [
  '_0' => 'no language',
  '_1' => 'no field instance',
  '_2' => 'resource'
];

$file = '../scala/languages-per-collections-groupped.txt';
$handle = fopen($file, "r");
if ($handle) {
  while (($line = fgets($handle)) !== false) {
    processLine($line);
  }
  fclose($handle);
} else {
  echo "ERROR: can not open file $file\n";
}

function processLine($line) {
  $origJson = sparkOutputToJson($line);
  if (!is_null($origJson)) {
    $collectionId = $origJson[0];
    $json = orderJson(restructureJson($origJson[1]));
    saveJson($collectionId, $json);
  } else {
    echo $line, "\n";
  }
}

function saveJson($collectionId, $json) {
  $fileName = '../../europeana-qa-r/json/' . $collectionId . '.languages.json';
  file_put_contents($fileName, json_encode($json));
}

function sparkOutputToJson($record) {
  $record = preg_replace('/CompactBuffer/', '', $record);
  $record = preg_replace('/([a-zA-Z_][a-zA-Z0-9_\-]*),/', '"$1",', $record);
  $record = str_replace(['(', ')'], ['[', ']'], $record);
  return json_decode($record);
}

function restructureJson($json) {
  global $codes;

  $fields = [];
  $aggregated = [];
  foreach ($json as $field) {
    list($fieldName, $langList) = $field;
    $languages = [];
    foreach ($langList as $lang) {
      list($language, $sum) = $lang;
      if (isset($codes[$lang[0]]))
        $language = $codes[$language];

      if (!isset($aggregated[$language]))
        $aggregated[$language] = 0;
      $aggregated[$language] += $sum;

      $languages[$language] = $sum;
    }
    $fields[$fieldName] = $languages;
  }
  $fields['aggregated'] = $aggregated;

  return $fields;
}

function orderJson($json) {
  global $order;

  $ordered = [];
  foreach ($order as $field) {
    arsort($json[$field]);
    $ordered[$field] = $json[$field];
  }
  return $ordered;
}
