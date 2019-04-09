<?php

$file = '../output/languages.csv';

$header = ['field', 'language', 'sum'];

$order = [
  'proxy_dc_title', 'proxy_dcterms_alternative', 'proxy_dc_description', 'proxy_dc_creator', 
  'proxy_dc_publisher', 'proxy_dc_contributor', 'proxy_dc_type', 'proxy_dc_identifier', 
  'proxy_dc_language', 'proxy_dc_coverage', 'proxy_dcterms_temporal', 'proxy_dcterms_spatial',
  'proxy_dc_subject', 'proxy_dc_date', 'proxy_dcterms_created', 'proxy_dcterms_issued',
  'proxy_dcterms_extent', 'proxy_dcterms_medium', 'proxy_dcterms_provenance',
  'proxy_dcterms_hasPart', 'proxy_dcterms_isPartOf', 'proxy_dc_format', 'proxy_dc_source',
  'proxy_dc_rights', 'proxy_dc_relation', 'proxy_edm_europeanaProxy', 'proxy_edm_year',
  'proxy_edm_userTag', 'proxy_ore_ProxyIn', 'proxy_ore_ProxyFor', 'proxy_dc_conformsTo',
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

$csv = array_map('str_getcsv', file($file));
array_walk($csv, function(&$a) use ($csv) {
  global $header, $codes;
  $a = array_combine($header, $a);
  if (isset($codes[$a['language']])) {
    $a['language'] = $codes[$a['language']];
  }
});

$fieldMap = [];
$languages = [];
foreach ($csv as $row) {
  if (!isset($languages[$row['language']])) {
    $languages[$row['language']] = 0;
  }
  $languages[$row['language']] += $row['sum'];
  $fieldMap[$row['field']][$row['language']] = $row['sum'];
}

arsort($languages);

$ordered = [];
$ordered['aggregated'] = $languages;
foreach ($order as $field) {
  if (isset($fieldMap[$field])) {
    arsort($fieldMap[$field]);
    $ordered[$field] = $fieldMap[$field];
  }
}

file_put_contents('../output/languages.json', json_encode($ordered));
