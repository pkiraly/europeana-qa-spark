<?php

define('FILENAME', 'version.txt');
define('DEFAULT_VERSION', 'v2018-08');

echo file_exists(FILENAME) ? file_get_contents(FILENAME) : DEFAULT_VERSION;