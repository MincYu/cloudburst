from cloudburst.client.ephe_common import *


def start_point(cloudburst):
    return

def sleep_only(cloudburst):
    import time
    time.sleep(1)
    return

def end_point(cloudburst):
    return 'Done'


test_ephe = False
NUM = 100

if test_ephe:
    pass
else:
    start_name = 'start_point'
    end_name = 'end_point'
    start_func = cloudburst_client.register(start_point, start_name)
    end_func = cloudburst_client.register(end_point, end_name)

    sleep_funcs = []
    for i in range(NUM):
        name = f'sleep_{i}'
        func = cloudburst_client.register(sleep_only, name)
        sleep_funcs.append(name)


    dag_name = 'dag_sleep'
    functions = [start_name, end_name] + sleep_funcs
    conns = [(start_name, name) for name in sleep_funcs] + [(name, end_name) for name in sleep_funcs]
    success, error = cloudburst_client.register_dag(dag_name, functions, conns)
    print(f'Create dag {dag_name} {success} {error}')

    elasped_list = []
    for _ in range(10):
        start = time.time()
        cloudburst_client.call_dag(dag_name, {}).get()
        end = time.time()
        elasped_list.append(end - start)
    print('dag results: elasped {}'.format(elasped_list))
    suc, err = cloudburst_client.delete_dag(dag_name)

