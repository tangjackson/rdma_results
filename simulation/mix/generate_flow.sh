#!/bin/bash

num_round=5
num_host=40
num_flow=$[3+$num_round*$num_host]

echo $num_flow

echo "4 5 3 100 200000000 0"
echo "8 6 3 100 200000000 0"
echo "9 7 3 100 200000000 0"


for (( i=0; i < $num_round; ++i ))
do
    t=$(echo "0.02+0.001*$i*10"|bc)
    for (( j=1; j <= $num_host; ++j ))
    do
        echo "$[9+$j] 6 3 100 100000 $t"
    done
done