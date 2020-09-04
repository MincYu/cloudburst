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
import os

from cloudburst.server.benchmarks import utils
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS
from cloudburst.shared.reference import CloudburstReference
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if len(sys.argv) < 2:
    print('Usage: ./client_test.py {workload} ')
    exit(1)

chosen_test = sys.argv[1]

f_elb = 'a962e279e1b764137aa7fc29ec53b34a-1353917714.us-east-1.elb.amazonaws.com'
my_ip = '3.236.45.49'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

if chosen_test == 'clean':
    all_func = cloudburst_client.list()
    for f in all_func:
        suc, err = cloudburst_client.delete_dag(f)

        print(f'Delete {f} {suc}')
    exit(0)

def write_test(cloudburst, size, keys):
    # import numpy as np
    # s = np.add(v1, v2)
    # s = np.add(s, v3)
    # s = np.add(s, v4)
    new_v = np.random.random(size)
    for key in keys:
        cloudburst.put(key, new_v)
    return

def read_test(cloudburst, keys):
    for key in keys:
        v = cloudburst.get(key)
    return

OSIZE = 1000
key_num = 4000
max_workers = 1
num_requests = 4

keys = [ 'v' + str(i) for i in range(key_num)]

def prepare_read():
    # refs = ()
    # k_v1, k_v2, k_v3, k_v4 = 'v1', 'v2', 'v3', 'v4'
    # if force or cloudburst_client.kvs_client.get('v0')['v0'] is None:
        # cloudburst_client.put_object(k_v1, v1)
        # cloudburst_client.put_object(k_v2, v2)
        # cloudburst_client.put_object(k_v3, v3)
        # cloudburst_client.put_object(k_v4, v4)
    # refs += (CloudburstReference(k_v1, True),)
    # refs += (CloudburstReference(k_v2, True),)
    # refs += (CloudburstReference(k_v3, True),)
    # refs += (CloudburstReference(k_v4, True),)

    if 'test_size' in os.environ and os.environ['test_size'] == str(OSIZE) and \
        'test_num' in os.environ and os.environ['test_num'] == str(key_num):
        print('detected data exist')
    else:
        val = np.random.random(OSIZE)
        for k in keys:
            cloudburst_client.put_object(k, val)
        print('data ready')
    
    os.environ['test_size'] = str(OSIZE)
    os.environ['test_num'] = str(key_num)
    return [keys]

def prepare_write():
    return [OSIZE, keys]

all_test_suite = {
    'read': ('read_test_', read_test, prepare_read),
    'write': ('write_test_', write_test, prepare_write)
}

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

    print(f'worker {tid} runtime info. mean: {np.average(total_time)}, std: {np.std(total_time)}, sche: {np.average(scheduler_time)}')
    # if total_time:
    #     utils.print_latency_stats(total_time, 'E2E')
    # if scheduler_time:
    #     utils.print_latency_stats(scheduler_time, 'SCHEDULER')
    # if kvs_time:
    #     utils.print_latency_stats(kvs_time, 'KVS')
    return total_time

def register(client, name, func, force=True):

    cloud_func = client.get_function(name)
    if force or cloud_func is None:
        cloud_func = client.register(func, name)
    
    client.register_dag(name, [name], [])
    return name

if chosen_test not in all_test_suite:
    print(f'No such workload {chosen_test}')
    exit(1)

name_pre, test_func, refs = all_test_suite[chosen_test][0],  all_test_suite[chosen_test][1], all_test_suite[chosen_test][2]()

all_clients = [cloudburst_client]
all_dags = [register(cloudburst_client, name_pre + '0', test_func)]
for i in range(1, max_workers):
    cur_client = CloudburstConnection(f_elb, my_ip, tid=i, local=False)
    all_clients.append(cur_client)
    all_dags.append(register(cur_client, name_pre + str(i), test_func))

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
pool = ThreadPoolExecutor(max_workers=max_workers)
futs = []
for tid, dag, client in zip(range(max_workers), all_dags, all_clients):
    # print({dag: refs})
    fu = pool.submit(exec_one, client, tid, dag, {dag: refs})
    futs.append(fu)

all_times = []
for fu in futs:
    all_times += fu.result()

print(f'Total runtime info. mean: {np.average(all_times)}, std: {np.std(all_times)}')