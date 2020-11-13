from cloudburst.client.client import CloudburstConnection

import random
import sys
import time
import uuid
import cloudpickle as cp
import numpy as np
import os


f_elb = 'a62eeb33f6b3b43779ea9c09796b7c2e-1388118431.us-east-1.elb.amazonaws.com'
my_ip = '18.212.101.17'

cloudburst_client = CloudburstConnection(f_elb, my_ip, tid=0, local=False)

# print(cloudburst_client.get('test'))
# exit(0)

# def test(cloudburst):
#     cloudburst.put('test', 'new', durable=True)
# test_func = cloudburst_client.register(test, 'test0')
# test_func()
# exit(0)

def preprocess(cloudburst, inp):
    from skimage import filters
    preprocessed = filters.gaussian(inp).reshape(1, 3, 224, 224)
    cloudburst.put(('pipeline', 'img', None), preprocessed, init_session=True, durable=False)

def sqnet(cloudburst, keys):
    import torch
    import torchvision

    for 
    model = torchvision.models.squeezenet1_1()
    result = model(torch.tensor(inp.astype(np.float32))).detach().numpy()

def write_test(cloudburst, name, size):    
    new_v = np.random.random(size)
    start_put = time.time()
    logging.info('Start writing')
    cloudburst.put('start', start_put, durable=True)

    cloudburst.put((name, 'v0', None), new_v, init_session=True, durable=False)

def read_test(cloudburst, data):
    logging.info('received data {}'.format(data))

    for d in data:
        if len(d) == 3:
            bucket, key, session = d
            v = cloudburst.get((bucket, key, session), durable=False)

    end = time.time()
    cloudburst.put('end', end, durable=True)


write_func = cloudburst_client.register(write_test, 'write_6')
read_func = cloudburst_client.register(read_test, 'read_6')

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

