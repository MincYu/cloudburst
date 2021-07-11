from cloudburst.client.ephe_common import *


# def dag_start(cloudburst, key, size):
#     return 1

def dag_sleep(cloudburst, v):
    time.sleep(1)
    end_t = time.time()
    return end_t

def dag_end(cloudburst, *values):
    return len(values)

test_ephe = False
SLEEP_NUM = 1

sleep_name = 'sleep'
end_name = 'end'
end_func = cloudburst_client.register(dag_end, end_name)

sleep_names = [ sleep_name + str(i) for i in range(SLEEP_NUM)]
for n in sleep_names:
    cloudburst_client.register(dag_sleep, n)

dag_name = 'parallel_sleep'
functions = sleep_names + [end_name]
conns = [(n, end_name) for n in sleep_names]
success, error = cloudburst_client.register_dag(dag_name, functions, conns)
print(f'Create dag {dag_name} {success} {error}')

arg_map = {n: [0] for n in sleep_names}
elasped_list = []
for _ in range(5):
    start = time.time()
    res = cloudburst_client.call_dag(dag_name, arg_map).get()
    end = time.time()
    print(res)
    elasped_list.append(end - start)
print('dag results: elasped {}'.format(elasped_list))
suc, err = cloudburst_client.delete_dag(dag_name)

