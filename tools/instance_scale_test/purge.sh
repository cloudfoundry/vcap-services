#!/bin/bash

base_dir=`dirname $0`
source $base_dir/config

$base_dir/run.sh delete_app
$base_dir/run.sh delete_user

