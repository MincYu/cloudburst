#!/bin/bash

# for KEY in 2000; do
#     for WORKER in 1 2 4; do
#         echo "Key num $KEY; Worker num $WORKER; Sequential"
#         python3 -m cloudburst.client.client_test write $KEY $WORKER 0
#     done
# done

SEQ=0
for KEY in 2000; do
    for WORKER in 1 2 4; do
        echo "Key num $KEY; Worker num $WORKER; Seq $SEQ"
        python3 -m cloudburst.client.client_test write $KEY $WORKER $SEQ
    done
done