# set -x

log_dir=/home/ubuntu/cb_logs
# rm -r $log_dir
mkdir -p $log_dir
prefix=$1

func_nodes=$(kubectl get pod | grep func | cut -d " " -f 1 | tr -d " ")

# sched_nodes=$(kubectl get pod | grep schedu | cut -d " " -f 1 | tr -d " ")
# for pod in ${sched_nodes[@]}; do
#     kubectl exec -it $pod -- tail -1000 hydro/cloudburst/log_scheduler.txt | grep 'App func' | tail -100 &> $log_dir/${prefix}k_sched.txt
# done

# for pod in ${func_nodes[@]}; do
#     for func_id in $(seq 1 20); do
#         result=`kubectl logs $pod -c function-${func_id} | tail -1`
#         count=`echo $result | grep 'function' | wc -l`
#         if [[ $count > '0' ]]; then
#             kubectl logs $pod -c function-${func_id} | tail -100 &> $log_dir/${prefix}k_${pod}_${func_id}.txt
#             # echo "func ${func_id} have data!!!"
#         fi
#     done
# done

# throughput
for pod in ${func_nodes[@]}; do
    for func_id in $(seq 1 20); do
        kubectl exec -it $pod -c function-${func_id} -- cat hydro/cloudburst/log_executor.txt | grep 'Executor timer. dag_exec_socket' &> $log_dir/${prefix}k_${pod}_${func_id}.txt
    done
done