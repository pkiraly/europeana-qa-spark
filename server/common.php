<?php

define('FILENAME', 'status.txt');
define('DEFAULT_STATUS', 'IDLE');

$task_list = [
  'IDLE',
  'MONGO_EXPORT', 'MONGO_UPLOAD',
  'SPARK_COMPLETENESS', 'SPARK_SATURATION', 'SPARK_LANGUAGE',
  'SPLIT_COMPLETENESS', 'SPLIT_SATURATION',
  'R_COMPLETENESS', 'R_SATURATION',
  'FINISHED'
];

$state_list = ['STARTED', 'FINISHED'];

function read_status() {
  return file_exists(FILENAME) ? file_get_contents(FILENAME) : DEFAULT_STATUS;
}

function parse_status($status) {
  if ($status == 'IDLE' || $status == 'FINISHED') {
      $task = $status;
      $state = FALSE;
  } else {
    list($task, $state) = explode(':', $status);
  }

  return (object)['task' => $task, 'state' => $state, 'raw' => $status];
}
