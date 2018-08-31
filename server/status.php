<?php
require_once('common.php');

switch ($_SERVER['REQUEST_METHOD']) {
  case 'GET': 
    $status = read_status();
    break;
  case 'DELETE':
    $status = DEFAULT_STATUS;
    file_put_contents(FILENAME, $status);
    break;
  case 'PUT': 
  case 'POST': 
    $input_status = file_get_contents("php://input");
    $is_valid_request = validate_request($input_status);
    if ($is_valid_request) {
      $status = $input_status;
      file_put_contents(FILENAME, $status);
    } else {
      $status = read_status();
    }
    break;
  default: break;
}

echo $status;

function validate_request($input_status) {
  global $task_list;

  $is_valid_request = FALSE;

  $requested_status = parse_status($input_status);

  if ($requested_status->raw == 'IDLE')
    return TRUE;

  if (in_array($requested_status->task, $task_list)) {
    $current_status = parse_status(read_status());

    if ($requested_status->raw == 'MONGO_EXPORT:STARTED' && $current_status->raw === 'IDLE')
      return TRUE;

    if ($requested_status->raw == 'FINISHED' && $current_status->raw === 'R_SATURATION:FINISHED')
      return TRUE;

    if ($requested_status->state == 'FINISHED') {
      if ($current_status->task == $requested_status->task
          && $current_status->state == 'STARTED') {
        $is_valid_request = TRUE;
      }
    } else {
      $i = array_search($current_status->task, $task_list);
      if ($task_list[$i + 1] == $requested_status->task 
          && $current_status->state == 'FINISHED') {
        $is_valid_request = TRUE;
      }
    }
  }
  return $is_valid_request;
}