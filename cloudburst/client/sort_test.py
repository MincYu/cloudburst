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
    print('Usage: ./sort_test.py {workload}')
    exit(1)

f_elb = 'acca9d681767a4ce5b29cf0b44bc287b-2030441321.us-east-1.elb.amazonaws.com'
my_ip = '34.204.195.6'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

chosen_test = sys.argv[1]

if chosen_test == 'clean':
    suc, err = cloudburst_client.delete_dag('map')
    print(f'Delete map {suc}. {err}')
    suc, err = cloudburst_client.delete_dag('reduce')
    print(f'Delete reduce {suc}. {err}')
    exit(0)

def prepare_sort_data():
    if FORCE_GEN_DATA == 0:
        key_num_in_batch = int(key_num / batch_size)
        for i in range(batch_size):
            val = np.random.random(OSIZE)
            keys = [f'o{k}' for k in range(i * key_num_in_batch, i * key_num_in_batch + key_num_in_batch)]
            vals = [val for _ in range(key_num_in_batch)]
            for k, v in zip(keys, vals):
                res = cloudburst_client.put_object(k, v)
                if not res[k]:
                    print(f'Put batch {i}, key: {k}, res: {res}')
            
        print('data ready')
    else:
        print('assume data exist')
    
    return [key_num]

def sort_map(cloudburst, start_idx, input_num, reduce_num):
    get_start = time.time()


    # keys = [ f'o{k}' for k in range(start_idx, start_idx + input_num)]
    # origin_data = cloudburst.get(keys)

    origin_data = {}
    for k in range(start_idx, start_idx + input_num):
        key = f'o{k}'
        origin_data[key] = cloudburst.get(key)
    
    get_clock = time.time() - get_start

    map_idx = int(start_idx / input_num)
    obj_num_per_reducer = int(input_num / reduce_num)

    reduce_keys = []
    reduce_data = []
    for i in range(reduce_num):
        for j in range(obj_num_per_reducer):
            reduce_keys.append(f'm{i}_{map_idx}_{j}')
            idx = start_idx + i*obj_num_per_reducer + j
            reduce_data.append(origin_data[f'o{idx}'])

    put_start = time.time()
    for k, d in zip(reduce_keys, reduce_data):
        cloudburst.put(k, d)
    # cloudburst.put(reduce_keys, reduce_data)
    put_clock = time.time() - put_start

    cloudburst.put(f'm_res{map_idx}', [get_clock, put_clock])
    return

def sort_reduce(cloudburst, reduce_idx, map_num, obj_num_per_reducer):
    get_start = time.time()

    # keys = [ f'm{reduce_idx}_{i}_{j}' for j in range(obj_num_per_reducer) for i in range(map_num)]
    # map_data = cloudburst.get(keys)

    map_data = {}
    for i in range(map_num):
        for j in range(obj_num_per_reducer):
            key = f'm{reduce_idx}_{i}_{j}'
            map_data[key] = cloudburst.get(key)
    
    get_clock = time.time() - get_start

    output_keys = [ f'o{i}' for i in range(reduce_idx * map_num * obj_num_per_reducer, (reduce_idx + 1) * map_num * obj_num_per_reducer)]
    output_data = []
    for i in range(map_num):
        for j in range(obj_num_per_reducer):
            idx = f'm{reduce_idx}_{i}_{j}'
            output_data.append(map_data[idx])
    
    put_start = time.time()

    for k, d in zip(output_keys, output_data):
        cloudburst.put(k, d)
    # cloudburst.put(output_keys, output_data)
    put_clock = time.time() - put_start

    cloudburst.put(f'r_res{reduce_idx}', [get_clock, put_clock])
    return

key_num = 1024
OSIZE = 1000000
batch_size = 8

FORCE_GEN_DATA = 1
prepare_sort_data()

# print(cloudburst_client.get(['o100', 'o1000']))
# exit(0)

map_num = reduce_num = 4
input_num = int(key_num / map_num)
map_args = {}
for i in range(map_num):
    map_args[f'mapper{i}'] = [input_num * i, input_num, reduce_num]

obj_num_per_reducer = int(input_num / reduce_num)
reduce_args = {}
for i in range(reduce_num):
    reduce_args[f'reducer{i}'] = [i, map_num, obj_num_per_reducer]

map_names = []
for i in range(map_num):
    name = f'mapper{i}'
    map_func = cloudburst_client.register(sort_map, name)
    print(f'register {name}')
    map_names.append(name)

reduce_names = []
for i in range(reduce_num):
    name = f'reducer{i}'
    reduce_func = cloudburst_client.register(sort_reduce, name)
    print(f'register {name}')
    reduce_names.append(name)

def get_runtime_info():
    m_res = []
    for i in range(map_num):
        m_res.append(cloudburst_client.get(f'm_res{i}'))
    r_res = []
    for i in range(reduce_num):
        r_res.append(cloudburst_client.get(f'r_res{i}'))
    return m_res, r_res



# input_num = 4
# obj_num_per_reducer = 2
# sort_map(cloudburst_client, input_num * 0, input_num, reduce_num)
# sort_map(cloudburst_client, input_num * 1, input_num, reduce_num)
# sort_reduce(cloudburst_client, 0, map_num, obj_num_per_reducer)
# sort_reduce(cloudburst_client, 1, map_num, obj_num_per_reducer)
# print(get_runtime_info())
# exit(0)


cloudburst_client.register_dag('map', map_names, [])
cloudburst_client.register_dag('reduce', reduce_names, [])

def exec_one():
    mstart = time.time()
    m_res = cloudburst_client.call_dag('map', map_args).get()
    mtime = time.time() - mstart

    rstart = time.time()
    r_res = cloudburst_client.call_dag('reduce', reduce_args).get()
    rtime = time.time() - rstart

    time.sleep(2)
    m_res, r_res = get_runtime_info()
   
    print(f'Runtime info. map: {mtime}, reduce: {rtime}, map_res: {m_res}, reduce_res: {r_res}')

for i in range(2):
    exec_one()