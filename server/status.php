<?php
require_once('common.php');

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