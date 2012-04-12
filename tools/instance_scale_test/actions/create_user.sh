#!/bin/bash
user=$1
passwd=$2
token=$3

base_dir=`dirname $0`
source $base_dir/../config

users="$VMC_USERS"

ret=`echo $users | grep $user`
if test -z "$ret"
then
  vmc add-user --email $user --passwd $passwd
else
  echo "$user exists."
fi

