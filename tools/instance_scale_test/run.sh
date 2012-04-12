#!/bin/bash

action=$1

if test -z "$action"
then
  echo "You should specify the action"
  exit 1
fi

base_dir=`dirname $0`
source $base_dir/config

cd $base_dir

if test -x $base_dir/actions/${action}.sh
then
  echo ""
  echo "Staring action: $action ..."

  if test "$action"="create_user" -o "$action"="delete_user"
  then
    export VMC_USERS=`vmc users`
  fi

  echo "ctrl-c to interrupt the action"

  trap interrupt INT

  function interrupt() {
    echo "*** Trapped CTRL-C ***"
    echo "JUST KILL ALL RUNNING JOBS, FIX ME ..."
    ps xa | grep "$base_dir" | grep "${action}"| awk '{print $1}'| grep -v grep | xargs kill
    exit 1
  }
  will_exit=0
  do_end=`expr $do_start + $concurrency - 1`
  if test $do_end -gt $do_finish
  then
    do_end=$do_finish
  fi

  echo "Started @ `date`"

  while test $do_end -le $do_finish
  do
    echo "($do_start $do_end) @ `date`"

    for i in `seq $do_start $do_end`
    do
      token="$token_dir/cfost_$i.token"
      email="${user_prefix}${service_type}${i}@vmware.com"
      cmd="$base_dir/actions/${action}.sh $email $user_passwd $token"
      log_file="$log_dir/cfost_${action}_$i.log"
      nohup $cmd 1>$log_file 2>&1 &
    done

    FAIL=0

    for job in `jobs -p`
    do
      echo $job
      wait $job || let "FAIL+=1"
    done

    if [ "$FAIL" == "0" ];
    then
      echo "DONE!"
    else
      echo "FAIL! ($FAIL)"
    fi

    if test $will_exit -eq 1
    then
      break
    else
      do_start=`expr $do_start + $concurrency `
      do_end=`expr $do_end + $concurrency`
      if test $do_end -ge $do_finish
      then
        do_end=$do_finish
        will_exit=1
      fi
    fi
  done

  echo  "Finished @ `date`"
else
  echo "The action script does not exist or it is not executable."
  cd -
  exit 1
fi

cd -
exit 0
