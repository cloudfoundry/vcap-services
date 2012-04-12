#!/bin/bash

base_dir=`dirname $0`
source $base_dir/config

vmc target $target_url
vmc login --email $admin_user --passwd $admin_password

$base_dir/run.sh create_user
$base_dir/run.sh create_app
$base_dir/run.sh funload_app

vmc logout
