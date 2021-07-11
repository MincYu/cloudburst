from cloudburst.client.client import *
from cloudburst.shared.serializer import Serializer

import logging
import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np
import os

from cloudburst.server.benchmarks import utils
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if len(sys.argv) < 5:
    print(f'arg size {len(sys.argv)} < 5')
    sys.exit(1)

f_elb = sys.argv[1]
my_ip = sys.argv[2]
thread = int(sys.argv[3])
req_per_thread = int(sys.argv[4])
# f_elb = 'abfb3d2ffc99042128b5021106d4f94b-212926308.us-east-1.elb.amazonaws.com'
# my_ip = '3.80.60.167'

cloudburst_clients = [ CloudburstConnection(f_elb, my_ip, tid=i, local=False) for i in range(thread)]

def dag_empty(cloudburst, v):
    start_t = time.time()
    print(f'Empty function start: {start_t}')
    return

dag_name = 'empty_func'
func = cloudburst_clients[0].register(dag_empty, 'empty')
success, error = cloudburst_clients[0].register_dag(dag_name, ['empty'], [])
print(f'Create dag {dag_name} {success} {error}')

sleep_time = 10
print(f'Sleep {sleep_time}s...')
time.sleep(sleep_time)

arg_map = {'empty': [0]}
dc = DagCall()
dc.name = dag_name
dc.consistency = NORMAL
args = [serializer.dump(0, serialize=False)]
al = dc.function_args['empty']
al.values.extend(args)

msg_str = dc.SerializeToString()
def run(cloudburst_client, req_num):
    start = time.time()
    for _ in range(req_num):
        cloudburst_client.dag_call_sock.send(msg_str)
        # cloudburst_client.dag_call_sock.recv()
        # r = GenericResponse()
        # r.ParseFromString(cloudburst_client.dag_call_sock.recv())
    end = time.time()
    return end - start

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
pool = ThreadPoolExecutor(max_workers=thread)
futs = []
for cloudburst_client in cloudburst_clients:
    fu = pool.submit(run, cloudburst_client, req_per_thread)
    futs.append(fu)

cur_index = 0
for fu in futs:
    elasped = fu.result()
    print('Client {} elasped {}'.format(cur_index, elasped))
    cur_index += 1

# suc, err = cloudburst_client.delete_dag(dag_name)

