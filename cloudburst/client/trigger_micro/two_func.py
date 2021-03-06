from cloudburst.client.ephe_common import *

def dag_write(cloudburst, key, size, use_str):
    if use_str:
        new_v = 'a' * size
    else:
        new_v = np.random.random(size)

    start_1 = time.time()
    cloudburst.put('start_', start_1, durable=True)
    return [new_v, key]

def dag_read(cloudburst, up_res):
    end_1 = time.time()
    cloudburst.put('end_', end_1, durable=True)

def ephe_write(cloudburst, name, key, size, use_str):    
    # if use_str:
    #     new_v = 'a' * size
    # else:
    #     new_v = np.random.random(size)
    # init_sess = True if 'session' in name else False

    bucket_key = f'{name}|{key}'
    new_v = cloudburst.gen_test_str(size, bucket_key)
    logging.info(f'Gen str with size {len(new_v)}')

    start_1 = time.time()
    # cloudburst.put((name, key, None), new_v, use_session=init_sess, durable=False)
    cloudburst.put_test_str(size, bucket_key)
    # logging.info(f'Put str size {size}')
    start_2 = time.time()

    cloudburst.put('start_1_' + key, start_1, durable=True)
    cloudburst.put('start_2_' + key, start_2, durable=True)

def ephe_read(cloudburst, *data):
    end_1 = time.time()
    bucket, key, session = data[0], data[1], data[2]
    # v = cloudburst.get((bucket, key, session), durable=False)
    bucket_key = f'{bucket}|{key}'
    res = cloudburst.get_test_str(bucket_key)
    end_2 = time.time()

    cloudburst.put('end_1_' + key, end_1, durable=True)
    cloudburst.put('end_2_' + key, end_2, durable=True)

if len(sys.argv) < 3:
    print('Usage: ./two_func.py {test_ephe} {osize} {num:optional}')
    exit(1)

test_ephe = sys.argv[1] == '0'
OSIZE = int(sys.argv[2])

iter_num = 4
if len(sys.argv) > 3:
    iter_num = int(sys.argv[3])

use_str = True
if len(sys.argv) > 4:
    use_str = sys.argv[4] == '0'

if test_ephe:
    print(f'Test Trigger-Cache with size {OSIZE}')
    write_func = cloudburst_client.register(ephe_write, 'write_1')
    read_func = cloudburst_client.register(ephe_read, 'trigger_upon_write')

    bucket_name = 'test_norm'
    elasped_list_1 = []
    elasped_list_2 = []
    elasped_list_3 = []
    # key_pre = ''.join(random.choices(string.ascii_uppercase + string.digits, k=2))
    key_pre = 's'
    for i in range(iter_num):
        # key_n = f'{key_pre}_{i}'
        key_n = f'two_func'
        cur_stamp = time.time()
        write_func(bucket_name, key_n, OSIZE, use_str)

        # print('Retriving results')
        retri_start = time.time()
        while True:
            if time.time() - retri_start > timeout:
                print(f'Retriving timeout at {key_n}.')
                break
            
            end_2 = cloudburst_client.get('end_2_' + key_n)
            if end_2 and end_2 > cur_stamp:
                start_1 = cloudburst_client.get('start_1_' + key_n)
                start_2 = cloudburst_client.get('start_2_' + key_n)
                end_1 = cloudburst_client.get('end_1_' + key_n)
                
                elasped_list_1.append([start_1, start_2, end_1, end_2])

                # elasped_1 = start_2 - start_1
                # elasped_2 = end_1 - start_2
                # # elasped_2 = end_1 - start_1
                # elasped_3 = end_2 - end_1

                # elasped_list_1.append(elasped_1)
                # elasped_list_2.append(elasped_2)
                # elasped_list_3.append(elasped_3)
                break
    print('ephe results. elasped {}'.format(elasped_list_1))
    # print('ephe results. elasped {}'.format([elasped_list_1, elasped_list_2, elasped_list_3]))
    # print('ephe results. elasped {}'.format([elasped_list_2, elasped_list_3]))
else:
    print(f'Test Default with size {OSIZE}')
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
    arg_map = {write_name: [key_n, OSIZE, use_str]}

    elasped_list = []
    for _ in range(iter_num):
        cloudburst_client.call_dag(dag_name, arg_map, True)
        start = cloudburst_client.get('start_')
        end = cloudburst_client.get('end_')
        elasped_list.append(end - start)
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)

