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

f_elb = 'a688b117387b040b19ab7071874fd6c9-1722544826.us-east-1.elb.amazonaws.com'
my_ip = '3.85.213.80'

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
    cloudburst.put('start_' + key, start_put, durable=True)

    cloudburst.put((name, key, None), new_v, init_session=init_sess, durable=False)

def read_test(cloudburst, *data):
    logging.info('received data {}'.format(data))

    key_n = ""
    for bucket, key, session in zip(data[0::3], data[1::3], data[2::3]):
        v = cloudburst.get((bucket, key, session), durable=False)
        key_n = key

    end = time.time()
    cloudburst.put('end_' + key_n, end, durable=True)


write_func = cloudburst_client.register(write_test, 'write_0')
read_func = cloudburst_client.register(read_test, 'trigger_upon_write')

key_n = 'k0'
write_func(bucket_name, key_n, OSIZE)

print('Retriving results')
retri_start = time.time()
while True:
    if time.time() - retri_start > timeout:
        print('Retriving timeout.')
        break
    
    end = cloudburst_client.get('end_' + key_n)
    if end:
        start = cloudburst_client.get('start_' + key_n)
        elasped = end - start
        print('Retrived results: elasped {}'.format(elasped))
        break

