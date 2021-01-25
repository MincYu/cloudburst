from cloudburst.client.ephe_common import *


def dag_start(cloudburst, key, size):
    return 1

def dag_sleep(cloudburst, up_res):
    import time
    import uuid
    time.sleep(1)
    uid = str(uuid.uuid4())
    return str({uid:1})

def dag_end(cloudburst, *values):
    return len(values)

test_ephe = False
SLEEP_NUM = 1

if test_ephe:
    pass
else:
    start_name = 'dag_start'
    sleep_name = 'dag_sleep'
    end_name = 'dag_end'
    start_func = cloudburst_client.register(dag_start, start_name)
    end_func = cloudburst_client.register(dag_end, end_name)

    sleep_names = [ sleep_name + str(i) for i in range(SLEEP_NUM)]
    for n in sleep_names:
        cloudburst_client.register(dag_sleep, n)

    dag_name = 'dag_parallel'
    functions = [start_name] + sleep_names + [end_name]
    conns = [(start_name, n) for n in sleep_names] + [(n, end_name) for n in sleep_names]
    success, error = cloudburst_client.register_dag(dag_name, functions, conns)
    print(f'Create dag {dag_name} {success} {error}')

    arg_map = {start_name: ['dag1', 1]}
    elasped_list = []
    for _ in range(5):
        start = time.time()
        res = cloudburst_client.call_dag(dag_name, arg_map).get()
        end = time.time()
        print(res)
        elasped_list.append(end - start)
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)

