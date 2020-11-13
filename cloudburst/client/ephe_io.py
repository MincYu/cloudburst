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

if len(sys.argv) < 3:
    print('Usage: ./ephe_io.py {bucket_name} {size} {optional:timeout}')
    exit(1)

timeout = 10
bucket_name = sys.argv[1]
OSIZE = int(sys.argv[2])
if len(sys.argv) > 3:
    timeout = int(sys.argv[3])

f_elb = 'a151d14dc968e42dfbf3b4de5439972c-1911842656.us-east-1.elb.amazonaws.com'
my_ip = '18.212.101.17'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

# print(cloudburst_client.get('test'))
# exit(0)

# def test(cloudburst):
#     cloudburst.put('test', 'new', durable=True)
# test_func = cloudburst_client.register(test, 'test0')
# test_func()
# exit(0)

def write_test(cloudburst, name, key, size):    
    new_v = np.random.random(size)
    init_sess = True if 'session' in name else False
    start_put = time.time()
    logging.info('Start writing')
    cloudburst.put('start', start_put, durable=True)

    cloudburst.put((name, key, None), new_v, init_session=init_sess, durable=False)

def read_test(cloudburst, data):
    logging.info('received data {}'.format(data))

    for d in data:
        if len(d) == 3:
            bucket, key, session = d
            v = cloudburst.get((bucket, key, session), durable=False)

    end = time.time()
    cloudburst.put('end', end, durable=True)


write_func = cloudburst_client.register(write_test, 'write_2')
read_func = cloudburst_client.register(read_test, 'read_2')

write_func(bucket_name, 'v2', OSIZE)

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
