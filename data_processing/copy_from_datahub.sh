#!/bin/bash


ssh playground
psql -d shared_mobility -a -f ~/tmp/1_create_schema_and_import_data.sql 
pg_dump -n 'bikesharing' shared_mobility > /tmp/shared_mobility.dump
exit

scp bbock@playground:/tmp/shared_mobility.dump ~/TEMP/shared_mobility.dump
createdb -T postgres shared_mobility
psql shared_mobility < ~/TEMP/shared_mobility.dump

ssh playground
rm /tmp/shared_mobility.dump
exit
