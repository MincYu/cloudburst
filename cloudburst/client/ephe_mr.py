from cloudburst.client.ephe_common import *

# suc, err = cloudburst_client.delete_dag('map')
# print(f'Delete map {suc} {err}')
# suc, err = cloudburst_client.delete_dag('reduce')
# print(f'Delete reduce {suc} {err}')

# suc, err = cloudburst_client.delete_dag('ephe_map')
# print(f'Delete ephe_map {suc} {err}')
# exit(0)

def gen_sort_data(num, mapper_num, reducer_num, sample_num):
    num_per_mapper = int(num / mapper_num)
    sample_per_mapper = int(sample_num / mapper_num)

    samples = []
    for i in range(mapper_num):
        map_input = np.random.random(num_per_mapper)
        cloudburst_client.put_object('in_{}'.format(i + 1), map_input)

        index = np.random.choice(map_input.shape[0], sample_per_mapper, replace=False)  
        samples.append(map_input[index])
    return np.concatenate(samples, axis=0)

re_gen = False
mapper_num = reducer_num = 4

if re_gen or cloudburst_client.get('split') is None:
    print('Gen data...')
    start = time.time()
    samples = gen_sort_data(1000000 * 128, mapper_num, reducer_num, 10000)
    end = time.time()
    print('gen: {}'.format(end - start))

    start = time.time()
    samples.sort()
    split_points = [samples[int(len(samples) * i / reducer_num)] for i in range(1, reducer_num)]
    cloudburst_client.put_object('split', split_points)
    end = time.time()

    print('samples {}: {}'.format(samples.size, end - start))

# print(cloudburst_client.get('split'))
# exit(0)

def dag_map(cloudburst, map_id):
    map_start_clock = time.time()

    map_key = 'in_{}'.format(map_id)
    origin_data = cloudburst.get(map_key)
    split_points = cloudburst.get('split')

    map_get_clock = time.time()

    map_res = []
    first_p = split_points[0]
    last_p = split_points[-1]

    map_res.append(origin_data[origin_data <= first_p])
    if len(split_points) >= 2:
        for p, n in zip(split_points[:-1], split_points[1:]):
            tmp = origin_data[origin_data > p]
            tmp = tmp[tmp <= n]
            map_res.append(tmp)
    
    map_res.append(origin_data[origin_data > last_p])

    map_filter_clock = time.time()

    mid_keys = ['{}_{}'.format(map_id, i + 1) for i in range(len(map_res))]
    map_put_clocks = []
    for k, d in zip(mid_keys, map_res):
        cloudburst.put(k, d)
        map_put_clocks.append(time.time())

    cloudburst.put('map_clock_{}'.format(map_id), [map_start_clock, map_get_clock, map_filter_clock] + map_put_clocks)

def dag_reduce(cloudburst, reduce_id, mapper_num):
    reduce_start_clock = time.time()

    reduce_get_clocks = []

    mid_keys = ['{}_{}'.format(i, reduce_id) for i in range(1, mapper_num + 1)]
    mid_data = []
    for k in mid_keys:
        mid_data.append(cloudburst.get(k))
        reduce_get_clocks.append(time.time())

    sorted_data = np.concatenate(mid_data, axis=0)
    sorted_data.sort()
    
    reduce_sort_clock = time.time()

    cloudburst.put('out_{}'.format(reduce_id), sorted_data)

    reduce_put_clock = time.time()

    cloudburst.put('reduce_clock_{}'.format(reduce_id), [reduce_start_clock, *reduce_get_clocks, reduce_sort_clock, reduce_put_clock])

def ephe_map(cloudburst, map_id):
    map_start_clock = time.time()

    map_key = 'in_{}'.format(map_id)
    origin_data = cloudburst.get(map_key)
    split_points = cloudburst.get('split')

    map_get_clock = time.time()

    map_res = []
    first_p = split_points[0]
    last_p = split_points[-1]

    map_res.append(origin_data[origin_data <= first_p])
    if len(split_points) >= 2:
        for p, n in zip(split_points[:-1], split_points[1:]):
            tmp = origin_data[origin_data > p]
            tmp = tmp[tmp <= n]
            map_res.append(tmp)
    
    map_res.append(origin_data[origin_data > last_p])

    map_filter_clock = time.time()

    bucket = ['r_{}'.format(i + 1) for i in range(len(map_res))]
    map_put_clocks = []
    for b, d in zip(bucket, map_res):
        cloudburst.put((b, str(map_id), None), d, durable=False)
        map_put_clocks.append(time.time())

    cloudburst.put('ephe_map_clock_{}'.format(map_id), [map_start_clock, map_get_clock, map_filter_clock] + map_put_clocks)

def ephe_reduce(cloudburst, *data):
    reduce_start_clock = time.time()

    reduce_get_clocks = []
    mid_data = []

    reduce_id = None
    for bucket, key, session in zip(data[0::3], data[1::3], data[2::3]):
        if reduce_id is None:
            reduce_id = bucket.split('_')[-1]
        reduce_input = cloudburst.get((bucket, key, session), durable=False)
        mid_data.append(reduce_input)
        reduce_get_clocks.append(time.time())
        

    sorted_data = np.concatenate(mid_data, axis=0)
    sorted_data.sort()
    
    reduce_sort_clock = time.time()

    cloudburst.put('out_{}'.format(reduce_id), sorted_data)

    reduce_put_clock = time.time()

    cloudburst.put('ephe_reduce_clock_{}'.format(reduce_id), [reduce_start_clock, *reduce_get_clocks, reduce_sort_clock, reduce_put_clock])

def get_runtime_info():
    m_res = []
    for i in range(1, mapper_num + 1):
        m_res.append(cloudburst_client.get(f'map_clock_{i}'))
    r_res = []
    for i in range(1, reducer_num + 1):
        r_res.append(cloudburst_client.get(f'reduce_clock_{i}'))
    return m_res, r_res

test_ephe = False

if test_ephe:
    map_names = []
    map_args = {}
    for i in range(1, mapper_num + 1):
        name = f'ephe_mapper{i}'
        map_func = cloudburst_client.register(ephe_map, name)
        map_names.append(name)
        map_args[name] = [i]

    reduce_names = []
    for i in range(1, reducer_num + 1):
        name = f'ephe_reducer{i}'
        reduce_func = cloudburst_client.register(ephe_reduce, name)
        reduce_names.append(name)

    cloudburst_client.register_dag('ephe_map', map_names, [])
    mstart = time.time()
    cloudburst_client.call_dag('ephe_map', map_args).get()
    mtime = time.time() - mstart
    
    retri_start = time.time()
    while True:
        ends = []
        for i in range(1, reducer_num + 1):
            ends.append(cloudburst_client.get(f'ephe_reduce_clock_{i}'))
        if all([end is not None for end in ends]):
            starts = []
            for i in range(1, mapper_num + 1):
                starts.append(cloudburst_client.get(f'ephe_map_clock_{i}'))
            if all([end[0] > start[0] for start, end in zip(starts, ends)]):
                # the same test, valid
                break
        time.sleep(1)

    print(f'map time: {mtime}')
    print(f'Mapper clock: {starts}')
    print(f'Reducer clock: {ends}')
    suc, err = cloudburst_client.delete_dag('map')

else:
    map_names = []
    map_args = {}
    for i in range(1, mapper_num + 1):
        name = f'mapper{i}'
        map_func = cloudburst_client.register(dag_map, name)
        # print(f'register {name}')
        map_names.append(name)
        map_args[name] = [i]

    reduce_names = []
    reduce_args = {}
    for i in range(1, reducer_num + 1):
        name = f'reducer{i}'
        reduce_func = cloudburst_client.register(dag_reduce, name)
        # print(f'register {name}')
        reduce_names.append(name)
        reduce_args[name] = [i, mapper_num]

    cloudburst_client.register_dag('map', map_names, [])
    cloudburst_client.register_dag('reduce', reduce_names, [])

    mstart = time.time()
    ret_map_ids = cloudburst_client.call_dag('map', map_args).get()
    print(f'map: {ret_map_ids}')
    mtime = time.time() - mstart

    rstart = time.time()
    ret_reduce_ids = cloudburst_client.call_dag('reduce', reduce_args).get()
    print(f'reduce: {ret_reduce_ids}')
    rtime = time.time() - rstart
    print(f'map time: {mtime}, reduce time: {rtime}')
    
    time.sleep(2)
    m_res, r_res = get_runtime_info()
    print(f'Mapper clock: {m_res}')
    print(f'Reducer clock: {r_res}')

    suc, err = cloudburst_client.delete_dag('map')
    suc, err = cloudburst_client.delete_dag('reduce')

"""
coord_addr = ''
client = CoordClient(coord_addr, None)

key_set = [ str(i) for i in range(1, mapper_num + 1)]
for i in range(1, reducer_num + 1):
    client.create_bucket(f'r_{i}', NORMAL)
    client.add_trigger(f'r_{i}', f't_{i}', BY_SET, {'function': f'ephe_reducer{i}', 'key_set': key_set})

"""