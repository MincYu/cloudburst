#!/bin/bash

cd $HYDRO_HOME/cloudburst

IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`

thread=$1
req_num=$2

python3 -m cloudburst.client.trigger_micro.empty $FUNCTION_ADDR $IP $thread $req_num
