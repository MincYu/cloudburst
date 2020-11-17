from cloudburst.client.ephe_common import *

# cloudburst_client.delete_dag('dag_math')
# exit(0)

# for i in range(10):
#     print(cloudburst_client.get(f'start_{i}'), cloudburst_client.get(f'end_{i}'))
# exit(0)

def incr(cloudburst, x):
    start = time.time()
    # cloudburst.put('start_', start, durable=True)
    return (x + 1, start)

def square(cloudburst, *data):
    x, start = data[0], data[1]
    res = x * x
    end = time.time()
    cloudburst.put('start_', start, durable=True)
    cloudburst.put('end_', end, durable=True)
    return res

def ephe_incr(cloudburst, x):    
    start = time.time()
    y = x + 1
    cloudburst.put(('incr', str(x), None), y, durable=False)
    cloudburst.put(f'start_{x}', start, durable=True)

def ephe_square(cloudburst, *data):
    bucket, key, session = data[0], data[1], data[2]
    x = cloudburst.get((bucket, key, session), durable=False)
    res = x * x
    
    end = time.time()
    cloudburst.put(f'end_{key}', end, durable=True)

test_ephe = True

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
                if start:
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

"""
Create bucket and add triggers for coordination.

coord_addr = ''
client = CoordClient(coord_addr, None)
client.create_bucket('incr', NORMAL)
client.add_trigger('incr', 't1', UPON_WRITE, {'function': 'ephe_square'})

"""