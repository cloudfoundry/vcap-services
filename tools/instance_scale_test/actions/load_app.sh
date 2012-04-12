#!/bin/bash
user=$1
passwd=$2
token=$3

base_dir=`dirname $0`
source $base_dir/../config

vmc target $target_url
vmc login --email $user --passwd $passwd --token-file $token

if test $? -eq 0
then
  echo "load application for $user"
  tag=`echo $user | sed s"/\./-DOT-/"g | sed s"/@/-AT-/"g`
  appname="app_${tag}"
  appdir="$base_dir/assets/$app_framework/$app_label"
  cd $appdir
     idx=0
     while test $idx -lt $appnum
     do
      real_appname="${appname}_${idx}"
      srv_idx=`expr $idx % $servicenum`
      real_servicename="${service_type}_${tag}_${srv_idx}"
      base_url="${real_appname}.${suggest_url}"
      base_url=`echo $base_url | awk '{print tolower($0)}'`
      vmc start $real_appname  --token-file $token
      $base_dir/check_app_status.sh $token $real_appname RUNNING
      if test $? -eq 0
      then
        echo "App $real_appname started"
      fi
      sleep 2
      # get size
      echo ""http://${base_url}/service/$service_type/dbeater/db/size""
      curl -X GET "http://${base_url}/service/$service_type/dbeater/db/size"
      # create the table
      echo "http://${base_url}/service/$service_type/dbeater/table/$table"
      curl -X PUT -d "table=$table" "http://${base_url}/service/$service_type/dbeater/table/$table"
      # load the data
      echo "http://${base_url}/service/$service_type/dbeater/$table/$mega"
      curl -X POST -d"$table" -d"$mega" "http://${base_url}/service/$service_type/dbeater/$table/$mega"
      # get size
      echo "http://${base_url}/service/$service_type/dbeater/db/size"
      curl -X GET "http://${base_url}/service/$service_type/dbeater/db/size"
      vmc stop $real_appname  --token-file $token
      let idx++
     done
  cd -
fi
