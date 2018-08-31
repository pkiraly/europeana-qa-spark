<?php
require_once('common.php');

$status = parse_status(read_status());

if ($status->raw == 'IDLE') {
  $next = 'MONGO_EXPORT:STARTED';
} else if ($status->raw == 'R_SATURATION:FINISHED') {
  $next = 'FINISHED';
} else if ($status->state == 'STARTED') {
  $next = $status->task . ':FINISHED';
} else {
  $i = array_search($status->task, $task_list);
  if (count($task_list) != $i-1) {
    $i++;
  }
  $next = $task_list[$i] . ':STARTED';
}

echo $next;