from cloudburst.client.client import CloudburstConnection

import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np
import os

f_elb = 'a96465165c86b4c4b98afd4fbc759e9a-1572993111.us-east-1.elb.amazonaws.com'
my_ip = '18.209.67.116'
timeout = 10
key_n = 'image9'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

# def test(cloudburst):
#     import torch
#     import torchvision

#     inp =  np.random.randn(1, 3, 224, 224)
#     model = torchvision.models.squeezenet1_1()
#     res = model(torch.tensor(inp.astype(np.float32))).detach().numpy()
#     cloudburst.put('res', res, durable=True)
#     return res.size

# test_func = cloudburst_client.register(test, 'tst_123')
# print(test_func().get())
# exit(0)

def preprocess(cloudburst, key):
    from skimage import filters
    start = time.time()
    cloudburst.put('start_' + key, start, durable=True)
    # inp =  np.random.randn(1, 224, 224, 3)
    inp = cloudburst.get(key, durable=True)
    preprocessed = filters.gaussian(inp).reshape(1, 3, 224, 224)
    cloudburst.put(('pre', key, None), preprocessed, init_session=True, durable=False)

def sqnet_1(cloudburst, *data):
    import torch
    import torchvision

    if len(data) >= 3:
        bucket, key, session = data[0], data[1], data[2]
        inp = cloudburst.get((bucket, key, session), durable=False)

        model = torchvision.models.squeezenet1_1()
        res = model(torch.tensor(inp.astype(np.float32))).detach().numpy()
        # res = np.random.randn(1000)
        cloudburst.put(('result', 're_1', session), res, durable=False)

def sqnet_2(cloudburst, *data):
    import torch
    import torchvision

    if len(data) >= 3:
        bucket, key, session = data[0], data[1], data[2]
        inp = cloudburst.get((bucket, key, session), durable=False)

        model = torchvision.models.squeezenet1_1()
        res = model(torch.tensor(inp.astype(np.float32))).detach().numpy()
        # res = np.random.randn(1000)
        cloudburst.put(('result', 're_2', session), res, durable=False)

def average(cloudburst, *data):
    import numpy as np
    inps = []
    session = None
    for bucket, key, session in zip(data[0::3], data[1::3], data[2::3]):
        inp = cloudburst.get((bucket, key, session), durable=False)
        inps.append(inp)
        key_n = key.split('_')[0]

    if session:
        end = time.time()
        cloudburst.put('end_' + session, end, durable=True)

pre_func = cloudburst_client.register(preprocess, 'pre')
m1_func = cloudburst_client.register(sqnet_1, 'm1')
m2_func = cloudburst_client.register(sqnet_2, 'm2')
avg_func = cloudburst_client.register(average, 'avg')

arr = np.random.randn(1, 224, 224, 3)
cloudburst_client.put_object(key_n, arr)
print(key_n + ' is put')

session = pre_func(key_n)

print('Retriving results')
retri_start = time.time()
while True:
    if time.time() - retri_start > timeout:
        print('Retriving timeout.')
        break
    
    end = cloudburst_client.get('end_' + session)
    if end:
        start = cloudburst_client.get('start_' + key_n)
        elasped = end - start
        print('Retrived results: elasped {}'.format(elasped))
        break

