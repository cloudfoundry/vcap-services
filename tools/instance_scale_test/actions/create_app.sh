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
  echo "creating application for $user"
  tag=`echo $user | sed s"/\./-DOT-/"g | sed s"/@/-AT-/"g`
  appname="app_${tag}"
  appdir="$base_dir/assets/$app_framework/$app_label"
  cd $appdir
     idx=0
     while test $idx -lt $appnum
     do
      real_appname="${appname}_${idx}"
      vmc push $real_appname --no-start  -n --token-file $token
      srv_idx=`expr $idx % $servicenum`
      real_servicename="${service_type}_${tag}_${srv_idx}"
      if test $srv_idx -lt $idx
      then
         #just bind service
         vmc bind $real_servicename $real_appname -n --token-file $token
      else
         #create new one
         vmc create-service $service_type $real_servicename $real_appname -n --token-file $token
      fi
      #vmc start $real_appname --token-file $token
      let idx++
     done
  cd -
fi

