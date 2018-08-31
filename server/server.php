<?php

$status_list = [
  'IDLE',
  'MONGO_EXPORT', 'MONGO_UPLOAD',
  'SPARK_COMPLETENESS', 'SPARK_SATURATION', 'SPARK_LANGUAGE',
  'SPLIT_COMPLETENESS', 'SPLIT_SATURATION',
  'R_COMPLETENESS', 'R_SATURATION',
];

define('FILENAME', 'status.txt');
define('DEFAULT_STATUS', 'IDLE');

switch ($_SERVER['REQUEST_METHOD']) {
  case 'GET': 
    $status = read_status();
    break;
  case 'DELETE':
    $status = 'IDLE';
    file_put_contents(FILENAME, $status);
    break;
  case 'PUT': 
  case 'POST': 
    $input_status = file_get_contents("php://input");
    if (in_array($input_status, $status_list)) {
      $status = $input_status;
      file_put_contents(FILENAME, $status);
    } else {
      $status = read_status();
    }
    break;
  default: break;
}

echo $status, "\n";

function read_status() {
  return file_exists(FILENAME) ? file_get_contents(FILENAME) : DEFAULT_STATUS;
}