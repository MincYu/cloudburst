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

f_elb = 'ac42ba7fd7a784f37bcb2d714af87fc8-1544868072.us-east-1.elb.amazonaws.com'
my_ip = '18.206.238.17'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

def test(cloudburst, a, b):
    import numpy as np
    s = np.add(a, b)
    key = 'sum'
    res = cloudburst.put(key, s)
    return res

name = 'test_2'
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
rid = cloudburst_client.call_dag(name, arg_map) 
# test_func(ref_a, ref_b)
print('scheduled')

# exit(0)

num_requests = 10
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

if total_time:
    utils.print_latency_stats(total_time, 'E2E')
if scheduler_time:
    utils.print_latency_stats(scheduler_time, 'SCHEDULER')
if kvs_time:
    utils.print_latency_stats(kvs_time, 'KVS')

# i = test_func(0).get()
# j = test_func(1).get()
# print(i, j)

# rids = []
# for i in range(2):
#     rids.append(test_func(i))
# print('scheduled')

# res = [ rid.get() for rid in rids]
# print(res)