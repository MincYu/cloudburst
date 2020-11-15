from cloudburst.client.client import CloudburstConnection

import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np
import os

f_elb = 'a9505900455b4485493d70e1200d7117-1477393795.us-east-1.elb.amazonaws.com'
my_ip = '54.196.208.3'
timeout = 10
key_n = 'image'

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

# inp = cloudburst_client.get(key_n)
# print(inp.size)
# exit(0)

session = cloudburst_client.exec_func('pre', [key_n])

print(f'Retriving results {session}')
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
    time.sleep(1)

"""
Create bucket and add triggers for coordination.

coord_addr = ''
client = CoordClient(coord_addr, None)

client.create_bucket('pre', SESSION)
client.create_bucket('result', SESSION)

client.add_trigger('pre', 't1', UPON_WRITE, {'function': 'm1'})
client.add_trigger('pre', 't2', UPON_WRITE, {'function': 'm2'})

client.add_trigger('result', 't3', BY_SET, {'function': 'avg', 'key_set': ['re_1', 're_2']})

"""