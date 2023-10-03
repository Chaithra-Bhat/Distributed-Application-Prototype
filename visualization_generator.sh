#!/bin/zsh


for j in {1..5}
do
    clientFileName="./pickles/latencies_client_num_$j.pkl"
    outputFileName="client_output_$j"
    python3 client.py --clientfname $clientFileName > $outputFileName &
done
wait