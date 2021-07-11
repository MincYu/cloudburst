#!/bin/bash

cd $HYDRO_HOME/cloudburst

IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`

elb=$1
thread=$2
req_num=$3

python3 -m cloudburst.client.trigger_micro.empty $elb $IP $thread $req_num
