#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/innoz/data/"
url="https://cloud.innoz.de/index.php/s/eQdoo4SIMHbV2Jg/download"

# #OPTIONAL: select data from datahub
# #import data from dataHub (MVG_Rad + others)
# ssh datahub
# psql -d shared_mobility <<EOF
# CREATE TABLE bikesharing.vehicle_sightings AS
# SELECT * FROM vehicle_sightings
# WHERE vehicle_type='bike' AND provider='mvg_rad';
# CREATE TABLE bikesharing.station_movements AS
# SELECT * FROM station_movements
# WHERE vehicle_type='bike' AND provider IN
# 	(
# 		'bay-area-bike-share',
# 		'bicing',
# 		'ford-gobike',
# 		'kvb rad germany',
# 		'kvb-rad',
# 		'kvb-rad-koln',
# 		'nextbike germany',
# 		'nextbike-berlin',
# 		'nextbike-frankfurt',
# 		'nextbike-hamburg',
# 		'nextbike-munchen',
# 		'velib'
# 	);
# CREATE TABLE bikesharing.grid AS
# SELECT * FROM grid;
# EOF
# \q
# pg_dump -n 'bikesharing' shared_mobility > /tmp/shared_mobility.dump
# exit
# scp bbock@datahub:/tmp/shared_mobility.dump ~/TEMP/shared_mobility.dump
# ssh datahub
# rm /tmp/shared_mobility.dump
# exit

#import dump from innoz-cloud
rm ${folder}shared_mobility.dump
wget ${url} -P ${folder}
#createdb -T postgres shared_mobility
psql shared_mobility < ${folder}shared_mobility.dump
