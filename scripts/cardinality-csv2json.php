<?php

$file = 'cardinality.csv';
$header = ['field', 'sum'];
$order = [
  'identifier', 'proxy_dc_title', 'proxy_dcterms_alternative', 'proxy_dc_description', 'proxy_dc_creator',
  'proxy_dc_publisher', 'proxy_dc_contributor', 'proxy_dc_type', 'proxy_dc_identifier', 'proxy_dc_language',
  'proxy_dc_coverage', 'proxy_dcterms_temporal', 'proxy_dcterms_spatial', 'proxy_dc_subject', 'proxy_dc_date',
  'proxy_dcterms_created', 'proxy_dcterms_issued', 'proxy_dcterms_extent', 'proxy_dcterms_medium',
  'proxy_dcterms_provenance', 'proxy_dcterms_hasPart', 'proxy_dcterms_isPartOf', 'proxy_dc_format',
  'proxy_dc_source', 'proxy_dc_rights', 'proxy_dc_relation', 'proxy_edm_isNextInSequence',
  'aggregation_edm_rights', 'aggregation_edm_provider', 'aggregation_edm_dataProvider',
  'aggregation_edm_isShownAt', 'aggregation_edm_isShownBy', 'aggregation_edm_object', 'aggregation_edm_hasView'
];

$csv = array_map('str_getcsv', file($file));
array_walk($csv, function(&$a) use ($csv) {
  global $header;
  $a = array_combine($header, $a);
});

$fieldMap = [];
foreach ($csv as $row) {
  $fieldMap[$row['field']] = $row;
}

$ordered = [];
foreach ($order as $field) {
  if (isset($fieldMap[$field])) {
    $ordered[] = $fieldMap[$field];
  }
}
print_r($ordered);
file_put_contents('cardinality.json', json_encode($ordered));
