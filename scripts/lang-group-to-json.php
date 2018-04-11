<?php

$start = microtime(TRUE);

$order = [
  'aggregated',
  'proxy_dc_title', 'proxy_dcterms_alternative', 'proxy_dc_description', 'proxy_dc_creator',
  'proxy_dc_publisher', 'proxy_dc_contributor', 'proxy_dc_type', 'proxy_dc_identifier',
  'proxy_dc_language', 'proxy_dc_coverage', 'proxy_dcterms_temporal', 'proxy_dcterms_spatial',
  'proxy_dc_subject', 'proxy_dc_date', 'proxy_dcterms_created', 'proxy_dcterms_issued',
  'proxy_dcterms_extent', 'proxy_dcterms_medium', 'proxy_dcterms_provenance',
  'proxy_dcterms_hasPart', 'proxy_dcterms_isPartOf', 'proxy_dc_format', 'proxy_dc_source',
  'proxy_dc_rights', 'proxy_dc_relation', 'proxy_edm_europeanaProxy', 'proxy_edm_year',
  'proxy_edm_userTag', 'proxy_ore_ProxyIn', 'proxy_ore_ProxyFor', 'proxy_dcterms_conformsTo',
  'proxy_dcterms_hasFormat', 'proxy_dcterms_hasVersion', 'proxy_dcterms_isFormatOf',
  'proxy_dcterms_isReferencedBy', 'proxy_dcterms_isReplacedBy', 'proxy_dcterms_isRequiredBy',
  'proxy_dcterms_isVersionOf', 'proxy_dcterms_references', 'proxy_dcterms_replaces',
  'proxy_dcterms_requires', 'proxy_dcterms_tableOfContents', 'proxy_edm_currentLocation',
  'proxy_edm_hasMet', 'proxy_edm_hasType', 'proxy_edm_incorporates', 'proxy_edm_isDerivativeOf',
  'proxy_edm_isRelatedTo', 'proxy_edm_isRepresentationOf', 'proxy_edm_isSimilarTo',
  'proxy_edm_isSuccessorOf', 'proxy_edm_realizes', 'proxy_edm_wasPresentAt',
  'aggregation_edm_rights', 'aggregation_edm_provider', 'aggregation_edm_dataProvider',
  'aggregation_dc_rights', 'aggregation_edm_ugc', 'aggregation_edm_aggregatedCHO',
  'aggregation_edm_intermediateProvider',
  'place_dcterms_isPartOf', 'place_dcterms_hasPart', 'place_skos_prefLabel', 'place_skos_altLabel',
  'place_skos_note',
  'agent_edm_begin', 'agent_edm_end', 'agent_edm_hasMet', 'agent_edm_isRelatedTo', 'agent_owl_sameAs',
  'agent_foaf_name', 'agent_dc_date', 'agent_dc_identifier', 'agent_rdaGr2_dateOfBirth',
  'agent_rdaGr2_placeOfBirth', 'agent_rdaGr2_dateOfDeath', 'agent_rdaGr2_placeOfDeath',
  'agent_rdaGr2_dateOfEstablishment', 'agent_rdaGr2_dateOfTermination', 'agent_rdaGr2_gender',
  'agent_rdaGr2_professionOrOccupation', 'agent_rdaGr2_biographicalInformation', 'agent_skos_prefLabel',
  'agent_skos_altLabel', 'agent_skos_note',
  'timespan_edm_begin', 'timespan_edm_end', 'timespan_dcterms_isPartOf', 'timespan_dcterms_hasPart',
  'timespan_edm_isNextInSequence', 'timespan_owl_sameAs', 'timespan_skos_prefLabel',
  'timespan_skos_altLabel', 'timespan_skos_note',
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

printf("Process took: %f ms\n", (microtime(TRUE) - $start));

function processLine($line) {
  $origJson = sparkOutputToJson($line);
  if (!is_null($origJson)) {
    $collectionId = $origJson[0];
    $json = orderJson(restructureJson($origJson[1]));
    saveJson($collectionId, $json);
  } else {
    echo "NULL line:\n", $line, "\n";
  }
}

function saveJson($collectionId, $json) {
  $fileName = '../../europeana-qa-r/json3/' . $collectionId . '/' . $collectionId . '.languages.json';
  file_put_contents($fileName, json_encode($json));
}

function sparkOutputToJson($record) {
  $record = preg_replace('/CompactBuffer/', '', $record);
  $record = preg_replace('/([a-zA-Z_][a-zA-Z0-9_\-]* ?),/', '"$1",', $record);
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
    if (isset($json[$field])) {
      arsort($json[$field]);
      $ordered[$field] = $json[$field];
    }
  }
  return $ordered;
}
