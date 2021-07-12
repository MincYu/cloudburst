# set -x

nodes=$(kubectl get pod | grep sched | cut -d " " -f 1 | tr -d " ")
for pod in ${nodes[@]}; do
    # kubectl get pod ${pod} -n default -o yaml | kubectl replace --force -f -
    kubectl exec -it ${pod} -- pkill -9 python
done

nodes=$(kubectl get pod | grep functi | cut -d " " -f 1 | tr -d " ")
for pod in ${nodes[@]}; do
    # kubectl exec -it $pod -c cache-container -- pkill -9 cache &> /dev/null &
    for func_id in $(seq 1 20); do
        kubectl exec -it $pod -c function-${func_id} -- pkill -9 python &> /dev/null &
    done
done
echo "waiting clear"
mana_nodes=$(kubectl get pod | grep management | cut -d " " -f 1 | tr -d " ")
for pod in ${mana_nodes[@]}; do
    kubectl exec -it $pod -- pkill -9 python
done

wait