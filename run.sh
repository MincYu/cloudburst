set -x

log_dir=/home/ubuntu/cb_logs
rm -r ${log_dir}

for i in 1 10 100 1000 10000; do
    bash clear.sh
    sleep 5
    size=$((i*1000))
    python3 -m cloudburst.client.trigger_micro.two_func $size 12
    bash collect.sh $i
done

bash clear.sh