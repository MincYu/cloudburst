from cloudburst.client.ephe_common import *

def dag_preprocess(cloudburst, key):
    from skimage import filters
    start = time.time()
    cloudburst.put('start_', start, durable=True)
    inp = cloudburst.get(key, durable=True)
    return filters.gaussian(inp).reshape(1, 3, 224, 224)

def dag_sqnet(cloudburst, inp):
    import torch
    import torchvision

    model = torchvision.models.squeezenet1_1()
    return model(torch.tensor(inp.astype(np.float32))).detach().numpy()

def dag_average(cloudburst, inp1, inp2):
    import numpy as np
    inp = [inp1, inp2]
    res = np.mean(inp, axis=0)

    end = time.time()
    cloudburst.put('end_', end, durable=True)
    return res

def preprocess(cloudburst, key):
    from skimage import filters
    start = time.time()
    cloudburst.put('start', start, use_session=True, durable=True)
    # inp =  np.random.randn(1, 224, 224, 3)
    inp = cloudburst.get(key, durable=True)
    preprocessed = filters.gaussian(inp).reshape(1, 3, 224, 224)
    cloudburst.put(('pre', key, None), preprocessed, use_session=True, durable=False)

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

    res = np.mean(inps, axis=0)

    if session:
        end = time.time()
        cloudburst.put('end_' + session, end, durable=True)

test_ephe = True

key_n = 'image'
arr = np.random.randn(1, 224, 224, 3)
cloudburst_client.put_object(key_n, arr)

if test_ephe:
    pre_func = cloudburst_client.register(preprocess, 'pre')
    m1_func = cloudburst_client.register(sqnet_1, 'm1')
    m2_func = cloudburst_client.register(sqnet_2, 'm2')
    avg_func = cloudburst_client.register(average, 'avg')

    elasped_list = []
    for _ in range(10):
        session = cloudburst_client.exec_func('pre', [key_n])

        # print(f'Retriving results {session}')
        retri_start = time.time()
        while True:
            if time.time() - retri_start > timeout:
                print('Retriving timeout.')
                break
            
            end = cloudburst_client.get('end_' + session)
            if end:
                start = cloudburst_client.get('start_' + session)
                elasped = end - start
                elasped_list.append(elasped)
                # print('Retrived results: elasped {}'.format(elasped))
                break
    print('ephe results. elasped {}'.format(elasped_list))
        
else:
    cloud_prep = cloudburst_client.register(dag_preprocess, 'preprocess')
    cloud_sqnet1 = cloudburst_client.register(dag_sqnet, 'sqnet1')
    cloud_sqnet2 = cloudburst_client.register(dag_sqnet, 'sqnet2')
    cloud_average = cloudburst_client.register(dag_average, 'average')

    dag_name = 'dag_pipeline'

    functions = ['preprocess', 'sqnet1', 'sqnet2', 'average']
    connections = [('preprocess', 'sqnet1'), ('preprocess', 'sqnet2'),
                ('sqnet1', 'average'), ('sqnet2', 'average')]
    success, error = cloudburst_client.register_dag(dag_name, functions, connections)
    
    arg_map = {'preprocess': [key_n]}

    elasped_list = []
    cloudburst_client.call_dag(dag_name, arg_map, True)
    for _ in range(10):
        cloudburst_client.call_dag(dag_name, arg_map, True)
        start = cloudburst_client.get('start_')
        end = cloudburst_client.get('end_')
        elasped_list.append(end - start)
    
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)

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