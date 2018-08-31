<?php

define('FILENAME', 'status.txt');
define('DEFAULT_STATUS', 'IDLE');

$status_list = [
  'IDLE',
  'MONGO_EXPORT', 'MONGO_UPLOAD',
  'SPARK_COMPLETENESS', 'SPARK_SATURATION', 'SPARK_LANGUAGE',
  'SPLIT_COMPLETENESS', 'SPLIT_SATURATION',
  'R_COMPLETENESS', 'R_SATURATION',
  'FINISHED'
];

function read_status() {
  return file_exists(FILENAME) ? file_get_contents(FILENAME) : DEFAULT_STATUS;
}