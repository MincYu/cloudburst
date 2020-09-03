#!/usr/bin/env python3

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from cloudburst.client.client import CloudburstConnection
from cloudburst.server.benchmarks import (
    centr_avg,
    composition,
    dist_avg,
    locality,
    mobilenet,
    predserving,
    scaling,
    summa,
    utils
)

import logging
import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS
from cloudburst.shared.reference import CloudburstReference
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

f_elb = 'aa33fb15a80c3441288cbe51058e44ac-683213724.us-east-1.elb.amazonaws.com'
my_ip = '34.239.170.193'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

def test(cloudburst, v1, v2, v3, v4):
    # import numpy as np
    # s = np.add(v1, v2)
    # s = np.add(s, v3)
    # s = np.add(s, v4)
    s = np.zeros(v1.size)
    key = 'sum'
    res = cloudburst.put(key, s)
    return res
    # import time
    # time.sleep(1)

OSIZE = 1000000
# cloudburst_client.delete_dag('test_0')
# exit(0)

def prepare_input():
    refs = ()
    k_v1, k_v2, k_v3, k_v4 = 'v1', 'v2', 'v3', 'v4'

    if cloudburst_client.kvs_client.get(k_v1)[k_v1] is None:
        v1 = np.zeros(OSIZE)
        v2 = np.zeros(OSIZE)
        v3 = np.ones(OSIZE)
        v4 = np.ones(OSIZE)

        cloudburst_client.put_object(k_v1, v1)
        cloudburst_client.put_object(k_v2, v2)
        cloudburst_client.put_object(k_v3, v3)
        cloudburst_client.put_object(k_v4, v4)

    refs += (CloudburstReference(k_v1, True),)
    refs += (CloudburstReference(k_v2, True),)
    refs += (CloudburstReference(k_v3, True),)
    refs += (CloudburstReference(k_v4, True),)
    
    return refs

num_requests = 10
def exec_one(cloudburst_client, tid, name, arg_map):
    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    for _ in range(num_requests):
        start = time.time()
        rid = cloudburst_client.call_dag(name, arg_map)
        end = time.time()

        stime = end - start

        start = time.time()
        rid.get()
        end = time.time()

        ktime = end - start

        total_time += [stime + ktime]
        scheduler_time += [stime]
        kvs_time += [ktime]


    print(f'{tid} runtime info: {np.average(total_time)} {np.average(scheduler_time)}')
    # if total_time:
    #     utils.print_latency_stats(total_time, 'E2E')
    # if scheduler_time:
    #     utils.print_latency_stats(scheduler_time, 'SCHEDULER')
    # if kvs_time:
    #     utils.print_latency_stats(kvs_time, 'KVS')

def register(client, name, func, force=True):

    cloud_func = client.get_function(name)
    if force or cloud_func is None:
        cloud_func = client.register(func, name)
    
    client.register_dag(name, [name], [])
    return name

max_workers = 4
name_pre = 'test_'
refs = prepare_input()

all_clients = [cloudburst_client]
all_dags = [register(cloudburst_client, name_pre + '0', test)]
for i in range(1, max_workers):
    cur_client = CloudburstConnection(f_elb, my_ip, tid=i, local=False)
    all_clients.append(cur_client)
    all_dags.append(register(cur_client, name_pre + str(i), test))

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
pool = ThreadPoolExecutor(max_workers=max_workers)
for tid, dag, client in zip(range(max_workers), all_dags, all_clients):
    pool.submit(exec_one, client, tid, dag, {dag: refs})