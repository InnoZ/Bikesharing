#!/bin/bash

psql -d shared_mobility -a -f data_processing/match_nextbike_stations_and_update_keys.sql
