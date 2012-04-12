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
  echo "$user does not exist, won't delete it."
else
  vmc delete-user $user -n
fi

