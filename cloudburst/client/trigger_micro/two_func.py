from cloudburst.client.ephe_common import *

def dag_write(cloudburst, size):
    start_1 = int(time.time() * 1000000)
    new_v = '1' * size
    start_2 = int(time.time() * 1000000)
    # cloudburst.put('start_', start_1, durable=True)
    print(f'Write function start: {start_1}, gen: {start_2}')
    return (new_v, start_1)

def dag_read(cloudburst, *up_res):
    up_time = up_res[1]
    end_1 = int(time.time() * 1000000)
    print(f'Read function start: {end_1}')
    return str(up_time) + '|' + str(end_1)

def dag_read_multi(cloudburst, *up_res):

    up_times = up_res[1::2]
    end_1 = int(time.time() * 1000000)
    print(f'Read function start: {end_1}')
    return '|'.join([str(t) for t in up_times]) + ',' + str(end_1)

def dag_end(cloudburst, *up_res):
    return ','.join(up_res)

def ephe_write(cloudburst, name, key, size, use_str):    
    if use_str:
        new_v = 'a' * size
    else:
        new_v = np.random.random(size)
    init_sess = True if 'session' in name else False

    start_1 = time.time()
    cloudburst.put((name, key, None), new_v, use_session=init_sess, durable=False)
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

if len(sys.argv) < 2:
    print('Usage: ./two_func.py {osize} {num:optional}')
    exit(1)

OSIZE = int(sys.argv[1])

iter_num = 8
if len(sys.argv) > 2:
    iter_num = int(sys.argv[2])

print(f'Test Default with size {OSIZE}')

fan_out = True
if fan_out:
    read_num = 1
    write_name = 'dag_write_1'
    read_names = [f'dag_read_{i}' for i in range(read_num)]
    cloudburst_client.register(dag_write, write_name)
    for n in read_names:
        cloudburst_client.register(dag_read, n)
    # cloudburst_client.register(dag_end, 'end_func')

    dag_name = 'dag_io'
    functions = [write_name] + read_names
    # functions = [write_name, 'end_func'] + read_names
    conns = [(write_name, n) for n in read_names]
    # conns = [(write_name, n) for n in read_names] + [(n, 'end_func') for n in read_names]
    success, error = cloudburst_client.register_dag(dag_name, functions, conns)
    print(f'Create dag {dag_name} {success} {error}')

    arg_map = {write_name: [OSIZE]}
    res = cloudburst_client.call_dag(dag_name, arg_map, True)
    print(res)

    elasped_list = []
    for _ in range(iter_num):
        res = cloudburst_client.call_dag(dag_name, arg_map, True)
        time.sleep(0.2)
        # print(res)
    #     all_reader_res = res.split(',')
    #     duras = []
    #     for r in all_reader_res:
    #         end = int(r.split('|')[1])
    #         start = int(r.split('|')[0])
    #         duras.append(end - start)

    #     elasped_list.append(max(duras))
    # print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)
else:
    write_num = 16
    write_names = [f'dag_write_{i}' for i in range(write_num)]
    read_name = 'dag_read'
    cloudburst_client.register(dag_read_multi, read_name)
    for n in write_names:
        cloudburst_client.register(dag_write, n)

    dag_name = 'dag_io'
    functions = [read_name] + write_names
    conns = [(n, read_name) for n in write_names]
    success, error = cloudburst_client.register_dag(dag_name, functions, conns)
    print(f'Create dag {dag_name} {success} {error}')

    arg_map = {n: [OSIZE] for n in write_names}
    res = cloudburst_client.call_dag(dag_name, arg_map, True)
    print(res)

    elasped_list = []
    for _ in range(iter_num):
        res = cloudburst_client.call_dag(dag_name, arg_map, True)
        # print(res)
        all_res = res.split(',')
        read_t = int(all_res[-1])
        write_ts = all_res[0].split('|')
        duras = []
        for write_t in write_ts:
            duras.append(read_t - int(write_t))

        elasped_list.append(max(duras))
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)