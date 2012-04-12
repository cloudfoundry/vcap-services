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
  echo "funload application for $user"
  tag=`echo $user | sed s"/\./-DOT-/"g | sed s"/@/-AT-/"g`
  appname="app_${tag}"
  appdir="$base_dir/assets/$app_framework/$app_label"
  real_appname="${appname}_0"
  vmc start $real_appname -n --token-file $token
  $base_dir/check_app_status.sh $token $real_appname RUNNING
  #vmc unbind-service "${service_type}_${tag}_0" $real_appname -n --token-file $token
  cd $appdir
     idx=0
     while test $idx -lt $appnum
     do
      if test $idx -ne 0
      then
         # reduce the resource usage of dae
         vmc stop "${appname}_${idx}" -n --token-file $token
         $base_dir/check_app_status.sh $token "${appname}_${idx}" STOPPED
      fi
      vmc start $real_appname -n --token-file $token
      $base_dir/check_app_status.sh $token $real_appname RUNNING
      # we will use the first app to bind other service to loading data ...
      srv_idx=`expr $idx % $servicenum`
      real_servicename="${service_type}_${tag}_${srv_idx}"
      if test $idx -ne 0
      then
         vmc bind-service $real_servicename $real_appname -n --token-file $token
         echo $?
      fi

      base_url="${real_appname}.${suggest_url}"
      base_url=`echo $base_url | awk '{print tolower($0)}'`
      # get size
      echo ""http://${base_url}/service/$service_type/dbeater/db/size""
      curl -X GET "http://${base_url}/service/$service_type/dbeater/db/size"
      # create the table
      echo "http://${base_url}/service/$service_type/dbeater/table/$table"
      curl -X PUT -d "table=$table" "http://${base_url}/service/$service_type/dbeater/table/$table"
      # load the data
      curl -X POST -d"$table" -d"$mega" "http://${base_url}/service/$service_type/dbeater/$table/$mega"
      # get size
      curl -X GET "http://${base_url}/service/$service_type/dbeater/db/size"
      #if test $idx -ne 0
      #then
      vmc unbind-service $real_servicename $real_appname -n --token-file $token
      echo $?
      #fi
      let idx++
     done
  cd -
  vmc bind-service "${service_type}_${tag}_0" $real_appname -n --token-file $token
fi
