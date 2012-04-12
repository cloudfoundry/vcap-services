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
      vmc start $real_appname --token-file $token
      $base_dir/check_app_status.sh $token $real_appname RUNNING
      if test $? -eq 0
      then
        echo "$real_appname started."
      else
        echo "$real_appname is not running."
      fi
      let idx++
    done
  cd -
fi

