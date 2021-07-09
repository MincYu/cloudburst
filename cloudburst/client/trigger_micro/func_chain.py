from cloudburst.client.ephe_common import *


def dag_inc(cloudburst, val):
    val += 1
    return val

INC_NUM = 10
func_names = [f'inc{i}' for i in range(INC_NUM)]
inc_funcs = [cloudburst_client.register(dag_inc, n) for n in func_names]
dag_name = 'func_chain'
conns = [(f'inc{i}', f'inc{i + 1}') for i in range(INC_NUM - 1)]
success, error = cloudburst_client.register_dag(dag_name, func_names, conns)
print(f'Create dag {dag_name} {success} {error}')

arg_map = {'inc0': [0]}
res = cloudburst_client.call_dag(dag_name, arg_map).get()
elasped_list = []
for _ in range(10):
    start = time.time()
    res = cloudburst_client.call_dag(dag_name, arg_map).get()
    end = time.time()
    # print(res)
    elasped_list.append(end - start)
print('dag results: elasped {}'.format(elasped_list))
suc, err = cloudburst_client.delete_dag(dag_name)

