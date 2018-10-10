<?php

function make_options($raw_array) {
  $shortOptions = implode('', array_keys($raw_array));
  $longOptions = array_values($raw_array);
  return [$shortOptions, $longOptions];
}