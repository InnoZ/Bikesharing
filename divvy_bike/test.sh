#!/bin/bash

db="-p 5432 -d postgres"
folder="/home/bbock/Repositories/Bikesharing/divvy_bike/data/"
url="https://s3.amazonaws.com/divvy-data/tripdata/"

readarray -t filenames < ${folder}filenames.csv

for filename in "${filenames[@]}"
do
  # get data for Chicago bikesharing
  #rm ${folder}${filename}.zip
  #wget ${url}${filename}.zip -P ${folder}
  #unzip ${folder}${filename}.zip -d ${folder}

  rm ${folder}${filename}*.zip
  rm ${folder}README*.txt
done
