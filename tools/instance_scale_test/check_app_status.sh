#!/bin/bash

status_ret=1
check_number=100

token=$1
app_name=$2
app_status=$3

i=1
while test $i -le $check_number
do
  check_ret=`vmc apps --token-file $token | grep $app_name | grep $app_status`
  if [ -z "$check_ret" ];
  then
  status_ret=1
  sleep 0.5
else
  exit 0
fi
let i++
done

exit $status_ret
