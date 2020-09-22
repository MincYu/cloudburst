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

from cloudburst.shared.proto.cloudburst_pb2 import (
    NORMAL,  # Cloudburst consistency modes
    MULTI,
)

from cloudburst.shared.serializer import Serializer

from anna.lattices import (
    MultiKeyCausalLattice,
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
    print('Usage: ./client_test.py {workload} {key_num} {worker} {sequence_exec}')
    exit(1)

chosen_test = sys.argv[1]

OSIZE = 1000000
key_num = 1
max_workers = 1
sequence_exec = False
consistency = NORMAL

if len(sys.argv) > 2:
    key_num = int(sys.argv[2])
if len(sys.argv) > 3:
    max_workers = int(sys.argv[3])
if len(sys.argv) > 4:
    sequence_exec = sys.argv[4] == '0'

num_requests = 100 * max_workers


f_elb = 'a8576b08f54cb4c0ebefb5f79a448438-342174087.us-east-1.elb.amazonaws.com'
my_ip = '34.204.195.6'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)


if chosen_test == 'clean':
    all_func = cloudburst_client.list()
    for f in all_func:
        suc, err = cloudburst_client.delete_dag(f)

        print(f'Delete {f} {suc}')
    exit(0)

def write_casual_test(cloudburst, size, key_num):
    exe_id = cloudburst.getid()
    new_v = np.random.random(size)
    result = serializer.dump_lattice(new_v, MultiKeyCausalLattice)

    keys = [ 'v' + str(i) for i in range(key_num)]
    start = time.time()

    res = [cloudburst.anna_client.causal_put(key, result, 0) for key in keys]

    end = time.time()
    return (end - start, all(res))

def write_test(cloudburst, size, key_num):    
    new_v = np.random.random(size)
    keys = [ 'v' + str(i) for i in range(key_num)]
    start_put = time.time()

    res = [cloudburst.put(key, new_v) for key in keys]

    end_put = time.time()
    return (end_put - start_put, all(res))

def read_test(cloudburst, key_num):
    keys = [ 'v' + str(i) for i in range(key_num)]
    start = time.time()
    
    for key in keys:
        v = cloudburst.get(key)

    end = time.time()
    return (end - start, v.size)

keys = [ 'v' + str(i) for i in range(key_num)]

FORCE_GEN_DATA = 0
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

    if FORCE_GEN_DATA == 0:
        val = np.random.random(OSIZE)
        for k in keys:
            cloudburst_client.put_object(k, val)
        print('data ready')
    else:
        print('assume data exist')
    
    return [key_num]

def prepare_write():
    return [OSIZE, key_num]

all_test_suite = {
    'read': ('read_server_', read_test, prepare_read, OSIZE),
    'write': ('write_server_', write_test, prepare_write, True),
    'cauwr': ('write_causal_1_', write_casual_test, prepare_write, True)
}

def exec_one(cloudburst_client, id_names, refs, deserved_flag):
    server_time = []
    tid_time = {}
    
    # ignore cold start
    for _, name in id_names:
        cloudburst_client.call_dag(name, {name: refs}, consistency=consistency).get()

    per_worker_reqs = int(num_requests / max_workers)
    print(f'Number of reqs per worker: {per_worker_reqs}')
    for _ in range(per_worker_reqs):

        for tid, name in id_names:
            start = time.time()
            
            rid = cloudburst_client.call_dag(name, {name: refs}, consistency=consistency)
            stime = time.time() - start            
            res = rid.get()
            # res = cloudburst_client.call_dag(name, {name: refs}, direct_response=True)
            # while res == None:
            #     res = cloudburst_client.call_dag(name, {name: refs}, direct_response=True)
            # stime = time.time() - start
            # print(res, time.time() - start)

            elasped, flag = res[0], res[1]
            if flag == deserved_flag:
                server_time.append(res[0])
            else:
                print(f'Attention: returned value {flag} cannot match {deserved_flag}')
            etime = time.time() - start

            tid_time[tid] = [(stime, etime)] + tid_time[tid] if tid in tid_time else [(stime, etime)]

            # time.sleep(1)

    for tid, total_time in tid_time.items():
        stimes, etimes = [i[0] for i in total_time], [i[1] for i in total_time]
        print(f'worker {tid} runtime info. mean: {np.average(etimes)}, std: {np.std(etimes)}. sche: {np.average(stimes)} ')
    # print(server_time)
    return server_time

def register(client, name, func, force=True, seq_num=1):

    cloud_func = client.get_function(name)
    if force or cloud_func is None:
        cloud_func = client.register(func, name)
    
    if seq_num == 1:
        client.register_dag(name, [name], [])
    else:
        seq = tuple([name for _ in range(seq_num)])
        client.register_dag(name, [name], [seq])
    return name

if chosen_test not in all_test_suite:
    print(f'No such workload {chosen_test}')
    exit(1)

name_pre, test_func, refs, deserved_flag = all_test_suite[chosen_test][0],  all_test_suite[chosen_test][1], all_test_suite[chosen_test][2](), all_test_suite[chosen_test][3]

all_times = []

if sequence_exec:
    id_names = []
    for i in range(max_workers):
        dag_name = register(cloudburst_client, name_pre + str(i), test_func)
        id_names.append((i, dag_name))

    all_times = exec_one(cloudburst_client, id_names, refs, deserved_flag)
else:
    # parallel
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
        fu = pool.submit(exec_one, client, [(tid, dag)], refs, deserved_flag)
        futs.append(fu)

    for fu in futs:
        all_times += fu.result()


print(f'Total server runtime. mean: {np.average(all_times)}, std: {np.std(all_times)}')