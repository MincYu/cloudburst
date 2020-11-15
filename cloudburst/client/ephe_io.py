from cloudburst.client.ephe_common import *

def dag_write(cloudburst, key, size):
    new_v = np.random.random(size)

    start_1 = time.time()
    cloudburst.put('start_', start_1, durable=True)
    return [new_v, key]

def dag_read(cloudburst, up_res):
    end_1 = time.time()
    cloudburst.put('end_', end_1, durable=True)

def ephe_write(cloudburst, name, key, size):    
    new_v = np.random.random(size)
    init_sess = True if 'session' in name else False

    start_1 = time.time()
    cloudburst.put((name, key, None), new_v, init_session=init_sess, durable=False)
    start_2 = time.time()

    cloudburst.put('start_1_' + key, start_1, durable=True)
    cloudburst.put('start_2_' + key, start_2, durable=True)

def ephe_read(cloudburst, *data):
    end_1 = time.time()
    bucket, key, session = data[0], data[1], data[2]
    v = cloudburst.get((bucket, key, session), durable=False)
    end_2 = time.time()

    cloudburst.put('end_1_' + key, end_1, durable=True)
    cloudburst.put('end_2_' + key, end_2, durable=True)

test_ephe = False
OSIZE = 1

if test_ephe:
    write_func = cloudburst_client.register(ephe_write, 'write_1')
    read_func = cloudburst_client.register(ephe_read, 'trigger_upon_write')

    bucket_name = 'test_norm'
    elasped_list_1 = []
    elasped_list_2 = []
    elasped_list_3 = []
    for i in range(10):
        key_n = f's1_{i}'
        write_func(bucket_name, key_n, OSIZE)

        # print('Retriving results')
        retri_start = time.time()
        while True:
            if time.time() - retri_start > timeout:
                print(f'Retriving timeout at {key_n}.')
                break
            
            end_2 = cloudburst_client.get('end_2_' + key_n)
            if end_2:
                start_1 = cloudburst_client.get('start_1_' + key_n)
                start_2 = cloudburst_client.get('start_2_' + key_n)
                end_1 = cloudburst_client.get('end_1_' + key_n)

                # elasped_1 = start_2 - start_1
                # elasped_2 = end_1 - start_2
                elasped_2 = end_1 - start_1
                elasped_3 = end_2 - end_1

                # elasped_list_1.append(elasped_1)
                elasped_list_2.append(elasped_2)
                elasped_list_3.append(elasped_3)
                break
    # print('ephe results. elasped {}'.format([elasped_list_1, elasped_list_2, elasped_list_3]))
    print('ephe results. elasped {}'.format([elasped_list_2, elasped_list_3]))
else:
    write_name = 'dag_write_1'
    read_name = 'dag_read_1'
    dag_write_func = cloudburst_client.register(dag_write, write_name)
    dag_read_func = cloudburst_client.register(dag_read, read_name)

    dag_name = 'dag_io'
    functions = [write_name, read_name]
    conns = [(write_name, read_name)]
    success, error = cloudburst_client.register_dag(dag_name, functions, conns)
    print(f'Create dag {dag_name} {success} {error}')

    key_n = 'dag1'
    arg_map = {write_name: [key_n, OSIZE]}

    elasped_list = []
    for _ in range(10):
        cloudburst_client.call_dag(dag_name, arg_map, True)
        start = cloudburst_client.get('start_')
        end = cloudburst_client.get('end_')
        elasped_list.append(end - start)
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)

