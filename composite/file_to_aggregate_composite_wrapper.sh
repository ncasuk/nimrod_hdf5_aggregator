#!/usr/bin/bash

incoming_directory=$1
output_file=$2

python_environment=/home/users/earjham/miniconda3/envs/moradar/bin/python

for incoming_file in $(ls -d ${incoming_directory}/*)
do
  echo ${incoming_file}
  ${python_environment} /home/users/earjham/bin/mo_radar_agg/composite/file_to_aggregate_composite.py ${incoming_file} ${output_file}
done