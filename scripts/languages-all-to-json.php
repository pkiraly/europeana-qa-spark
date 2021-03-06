<?php

$start = microtime(TRUE);
$inputFile = $argv[1];
$baseOutputDir = $argv[2];

if (!file_exists($baseOutputDir)) {
  die("Non existing directory: $baseOutputDir\n");
}

if (!file_exists($baseOutputDir . '/json')) {
  die("Directory should have a json subdirectory: $baseOutputDir does not have it.\n");
}

$order = [
  'aggregated',
  'proxy_dc_title', 'proxy_dcterms_alternative', 'proxy_dc_description', 'proxy_dc_creator',
  'proxy_dc_publisher', 'proxy_dc_contributor', 'proxy_dc_type', 'proxy_dc_identifier',
  'proxy_dc_language', 'proxy_dc_coverage', 'proxy_dcterms_temporal', 'proxy_dcterms_spatial',
  'proxy_dc_subject', 'proxy_dc_date', 'proxy_dcterms_created', 'proxy_dcterms_issued',
  'proxy_dcterms_extent', 'proxy_dcterms_medium', 'proxy_dcterms_provenance',
  'proxy_dcterms_hasPart', 'proxy_dcterms_isPartOf', 'proxy_dc_format', 'proxy_dc_source',
  'proxy_dc_rights', 'proxy_dc_relation', 'proxy_dcterms_conformsTo', 'proxy_dcterms_hasFormat',
  'proxy_dcterms_hasVersion', 'proxy_dcterms_isFormatOf', 'proxy_dcterms_isReferencedBy',
  'proxy_dcterms_isReplacedBy', 'proxy_dcterms_isRequiredBy', 'proxy_dcterms_isVersionOf',
  'proxy_dcterms_references', 'proxy_dcterms_replaces', 'proxy_dcterms_requires',
  'proxy_dcterms_tableOfContents', 'proxy_edm_currentLocation', 'proxy_edm_hasType',
  'proxy_edm_isRelatedTo',
  'aggregation_edm_provider', 'aggregation_edm_dataProvider', 'aggregation_dc_rights',
  'aggregation_edm_intermediateProvider',
  'place_dcterms_isPartOf', 'place_dcterms_hasPart', 'place_skos_prefLabel', 'place_skos_altLabel',
  'place_skos_note',
  'agent_edm_begin', 'agent_edm_end', 'agent_foaf_name', 'agent_dc_date', 'agent_dc_identifier',
  'agent_rdaGr2_dateOfBirth', 'agent_rdaGr2_placeOfBirth', 'agent_rdaGr2_dateOfDeath',
  'agent_rdaGr2_placeOfDeath', 'agent_rdaGr2_dateOfEstablishment', 'agent_rdaGr2_dateOfTermination',
  'agent_rdaGr2_gender', 'agent_rdaGr2_professionOrOccupation', 'agent_rdaGr2_biographicalInformation',
  'agent_skos_prefLabel', 'agent_skos_altLabel', 'agent_skos_note',
  'timespan_edm_begin', 'timespan_edm_end', 'timespan_dcterms_isPartOf', 'timespan_dcterms_hasPart',
  'timespan_edm_isNextInSequence', 'timespan_skos_prefLabel', 'timespan_skos_altLabel', 'timespan_skos_note',
  'concept_skos_broader', 'concept_skos_narrower', 'concept_skos_related',
  'concept_skos_broadMatch', 'concept_skos_narrowMatch', 'concept_skos_relatedMatch',
  'concept_skos_exactMatch', 'concept_skos_closeMatch', 'concept_skos_notation',
  'concept_skos_inScheme', 'concept_skos_prefLabel', 'concept_skos_altLabel', 'concept_skos_note'
];

$codes = [
  '_0' => 'no language',
  '_1' => 'no field instance',
  '_2' => 'resource'
];

$csvFields = ["id", "field", "language", "occurences", "records"];

// $file = '../output/languages-all.csv';
$handle = fopen($inputFile, "r");
if ($handle) {
  $collection = null;
  while (($line = fgets($handle)) !== false) {
    $record = processLine($line);
    if (is_null($collection)) {
      $collection = createCollection($record->id);
    } else {
      if ($collection->id != $record->id) {
        // $json = orderJson(restructureJson($origJson[1]));
        saveJson($collection->id, orderJson($collection->fields));
        $collection = createCollection($record->id);
      }
    }
    if (!isset($collection->fields->{$record->field})) {
      $collection->fields->{$record->field} = [];
    }
    $collection->fields->{$record->field}[$record->language] = (object)[
      "occurences" => $record->occurences,
      "records" => $record->records
    ];
  }
  fclose($handle);
} else {
  echo "ERROR: can not open file $inputFile\n";
}

saveJson($collection->id, orderJson($collection->fields));
printf("Process took: %s\n", formatDuration(microtime(TRUE) - $start));

function createCollection($id) {
  $collection = (object)[
    'id' => $id,
    'fields' => (object)[]
  ];
  return $collection;
}

function processLine($line) {
  global $csvFields, $codes;

  $values = str_getcsv($line);
  $record = (object)array_combine($csvFields, $values);
  if ($record->field == 'all') {
    $record->field = 'aggregated';
  }
  if (isset($codes[$record->language])) {
    $record->language = $codes[$record->language];
  }
  return $record;
}

function saveJson($collectionId, $json) {
  global $baseOutputDir;

  $fileName = $baseOutputDir . '/json/' . $collectionId . '/' . $collectionId . '.languages-all.json';
  echo $fileName, "\n";
  file_put_contents($fileName, json_encode($json));
}

function formatDuration($microtime) {
  $totalsec = floor($microtime);
  $ms = substr(((string) $microtime - $totalsec), 2);
  $sec = $totalsec % 60;
  $min = ($totalsec / 60) %60;
  $hour = floor($totalsec / (60*60));
  return sprintf("%02d:%02d:%02d.%s", $hour, $min, $sec, $ms);
}

function orderJson($json) {
  global $order;

  $ordered = [];
  foreach ($order as $field) {
    if (isset($json->{$field})) {
      arsort($json->{$field});
      $ordered[$field] = $json->{$field};
    }
  }
  return $ordered;
}
