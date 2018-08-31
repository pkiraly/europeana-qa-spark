<?php
require_once('common.php');

$status = read_status();

$i = array_search($status, $status_list);
if (count($status_list) != $i-1) {
  $i++;
}
echo $status_list[$i], "\n";
