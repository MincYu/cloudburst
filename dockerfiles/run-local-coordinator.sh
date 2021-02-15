#!/bin/bash

cd $EPHE_HOME/coordinator
rm -rf conf
mkdir -p conf

IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`

cd build && make -j4 && cd ..
bash scripts/compile.sh

touch conf/config.yml
echo -e "threads:" | tee -a conf/config.yml
echo -e "    routing: 1" | tee -a conf/config.yml

echo -e "routing:" | tee -a conf/config.yml
echo -e "    ip: $IP" | tee -a conf/config.yml

echo -e "invoc:" | tee -a conf/config.yml
echo -e "    queue: /tmp/invoc" | tee -a conf/config.yml
echo -e "    platform: Cloudburst" | tee -a conf/config.yml

./build/target/coordinator &




