set -x

sender_nodes=$(kubectl get pod | grep benchmark | cut -d " " -f 1 | tr -d " ")
for pod in ${sender_nodes[@]}; do
    kubectl exec -it $pod -- bash /hydro/cloudburst/start-client-test.sh 400 500 &
done

wait