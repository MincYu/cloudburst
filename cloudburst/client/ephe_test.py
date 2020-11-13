from cloudburst.client.client import CloudburstConnection
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

if len(sys.argv) < 4:
    print('Usage: ./ephe_test.py {f_elb} {bucket_name} {size} {optional:timeout}')
    exit(1)

timeout = 10
f_elb = sys.argv[1]
bucket_name = sys.argv[2]
OSIZE = int(sys.argv[3])
if len(sys.argv >= 4):
    timeout = int(sys.argv[4])

my_ip = '18.212.101.17'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

def write_test(cloudburst, name, size):    
    new_v = np.random.random(size)
    start_put = time.time()
    cloudburst.put('start', start_put, durable=True)

    cloudburst.put((name, 'v1', None), new_v, init_session=True, durable=False)

def read_test(cloudburst, data):
    print('received data {}'.format(data))

    for d in data:
        if len(d) == 3:
            bucket, key, session = d
            v = cloudburst.get((bucket, key, session), durable=False)

    end = time.time()
    cloudburst.put('end', end, durable=True)


write_func = cloudburst_client.register(write_test, 'write')
read_func = cloudburst_client.register(read_test, 'read')

write_func(bucket_name, OSIZE)

print('Retriving results')
retri_start = time.time()
while True:
    if time.time() - retri_start > timeout:
        print('Retriving timeout.')
        break
    
    end = cloudburst_client.get('end')
    if end:
        start = cloudburst_client.get('start')
        elasped = end - start
        print('Retrived results: elasped {}'.format(elasped))

