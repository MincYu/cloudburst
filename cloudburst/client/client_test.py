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

f_elb = 'ada8a56372a614d2dbbc201653a6016b-1438011676.us-east-1.elb.amazonaws.com'
my_ip = '3.236.208.240'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

def test(cloudburst, a, b):
    # import numpy as np
    # s = np.add(a, b)
    # key = 'sum'
    # res = cloudburst.put(key, s)
    # return res
    import time
    time.sleep(1)

name = 'test_1'
# test_func = cloudburst_client.get_function('test')
# if test_func is None:
    # print('register func')
test_func = cloudburst_client.register(test, name)

cloudburst_client.register_dag(name, [name], [])

OSIZE = 10000000
a = np.zeros(OSIZE)
b = np.ones(OSIZE)
k_a = 'zero'
cloudburst_client.put_object(k_a, a)
ref_a = CloudburstReference(k_a, True)

k_b = 'one'
cloudburst_client.put_object(k_b, b)
ref_b = CloudburstReference(k_b, True)

arg_map = {name: (ref_a, ref_b)}
print('Prepared')

# rid = cloudburst_client.call_dag(name, arg_map) 
# test_func(ref_a, ref_b)

def exec_one(cloudburst_client, tid, name, arg_map):
    num_requests = 1
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


max_workers = 10
all_clients = [cloudburst_client]
for i in range(1, max_workers):
    all_clients.append(CloudburstConnection(f_elb, my_ip, tid=i, local=False))

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
pool = ThreadPoolExecutor(max_workers=max_workers)
for tid, client in zip(range(max_workers), all_clients):
    pool.submit(exec_one, client, tid, name, arg_map)

# i = test_func(0).get()
# j = test_func(1).get()
# print(i, j)

# rids = []
# for i in range(2):
#     rids.append(test_func(i))
# print('scheduled')

# res = [ rid.get() for rid in rids]
# print(res)