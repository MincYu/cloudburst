from cloudburst.client.client import CloudburstConnection

import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np
import os

f_elb = 'a80bcca2e05cb4022b0dba4cb54581d0-1701214262.us-east-1.elb.amazonaws.com'
my_ip = '18.209.67.116'
timeout = 10
key_n = 'image9'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

# from skimage import filters
# inp = cloudburst_client.get(key_n)
# preprocessed = filters.gaussian(inp).reshape(1, 3, 224, 224)
# exit(0)

# def test(cloudburst, key):
#     v = np.random.random(100)
#     cloudburst.put(key, v, durable=True)
    
#     read_v = cloudburst.get(key, durable=True)
#     logging.info(read_v.size)
# test_func = cloudburst_client.register(test, 'tst_100')
# test_func('tst_100')
# print(cloudburst_client.get('tst_100').size)
# exit(0)

def preprocess(cloudburst, key):
    from skimage import filters
    start = time.time()
    cloudburst.put('start_' + key, start, durable=True)
    inp =  np.random.randn(1, 224, 224, 3)
    # inp = cloudburst.get(key, durable=True)
    # logging.info(f'key: {key}, inp type: {type(inp)}')
    preprocessed = filters.gaussian(inp).reshape(1, 3, 224, 224)
    cloudburst.put(('pre', key, None), preprocessed, init_session=True, durable=False)

def sqnet_1(cloudburst, *data):
    # import torch
    # import torchvision

    if len(data) >= 3:
        bucket, key, session = data[0], data[1], data[2]
        inp = cloudburst.get((bucket, key, session), durable=False)

        # model = torchvision.models.squeezenet1_1()
        # res = model(torch.tensor(inp.astype(np.float32))).detach().numpy()
        res = np.random.randn(1000)
        cloudburst.put(('result', key + '_1', session), res, durable=False)

def sqnet_2(cloudburst, *data):
    # import torch
    # import torchvision

    if len(data) >= 3:
        bucket, key, session = data[0], data[1], data[2]
        inp = cloudburst.get((bucket, key, session), durable=False)

        # model = torchvision.models.squeezenet1_1()
        # res = model(torch.tensor(inp.astype(np.float32))).detach().numpy()
        res = np.random.randn(1000)
        cloudburst.put(('result', key + '_2', session), res, durable=False)

def average(cloudburst, *data):
    import numpy as np
    inps = []
    key_n = ""
    for bucket, key, session in zip(data[0::3], data[1::3], data[2::3]):
        inp = cloudburst.get((bucket, key, session), durable=False)
        inps.append(inp)
        key_n = key.split('_')[0]

    end = time.time()
    cloudburst.put('end_' + key_n, end, durable=True)

pre_func = cloudburst_client.register(preprocess, 'pre')
m1_func = cloudburst_client.register(sqnet_1, 'm1')
m2_func = cloudburst_client.register(sqnet_2, 'm2')
avg_func = cloudburst_client.register(average, 'avg')

# arr = np.random.randn(1, 224, 224, 3)
# cloudburst_client.put_object(key_n, arr)
# print(key_n + ' is put')

pre_func(key_n)

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

