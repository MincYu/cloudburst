from cloudburst.client.ephe_common import *

def incr(cloudburst, x):
    start = time.time()
    cloudburst.put('start_', start, durable=True)
    return x + 1

def square(cloudburst, x):
    res = x * x
    end = time.time()
    cloudburst.put('end_', end, durable=True)
    return res

def ephe_incr(cloudburst, x):    
    start = time.time()
    cloudburst.put(f'start_{x}', start, durable=True)
    y = x + 1

    cloudburst.put(('incr', str(x), None), y, durable=False)

def ephe_square(cloudburst, *data):
    bucket, key, session = data[0], data[1], data[2]
    v = cloudburst.get((bucket, key, session), durable=False)
    res = x * x
    
    end = time.time()
    cloudburst.put(f'end_{key}', end, durable=True)

test_ephe = False

if test_ephe:
    incr_func = cloudburst_client.register(ephe_incr, 'ephe_incr')
    squa_func = cloudburst_client.register(ephe_square, 'ephe_square')

    elasped_list = []
    for i in range(10):
        incr_func(i)

        retri_start = time.time()
        while True:
            if time.time() - retri_start > timeout:
                print(f'Retriving timeout at {i}.')
                break
            
            end = cloudburst_client.get(f'end_{i}')
            if end:
                start = cloudburst_client.get(f'start_{i}')
                elasped = end - start
                elasped_list.append(elasped)
                break
    print('ephe results. elasped {}'.format(elasped_list))
else:
    inc_name = 'dag_incr'
    suq_name = 'dag_squa'
    dag_inc_func = cloudburst_client.register(incr, inc_name)
    dag_suq_func = cloudburst_client.register(square, suq_name)

    dag_name = 'dag_math'
    functions = [inc_name, suq_name]
    conns = [(inc_name, suq_name)]
    success, error = cloudburst_client.register_dag(dag_name, functions, conns)
    print(f'Create dag {dag_name} {success} {error}')

    elasped_list = []
    for i in range(10):
        arg_map = {inc_name: [i]}
        cloudburst_client.call_dag(dag_name, arg_map, True)
        start = cloudburst_client.get('start_')
        end = cloudburst_client.get('end_')
        elasped_list.append(end - start)
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)

