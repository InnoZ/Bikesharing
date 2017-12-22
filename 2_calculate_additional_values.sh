#!/bin/bash

psql -d shared_mobility -a -f data_processing/calculate_additional_values.sql
psql -d shared_mobility -a -f data_processing/calculate_metastats.sql
psql -d shared_mobility -a -f data_processing/highchart.sql
psql -d shared_mobility -a -f data_processing/generate_weekly_curves.sql
# calculate nextbike specific values
psql -d shared_mobility -a -f data_processing/detect_station_observation_periods.sql
psql -d shared_mobility -a -f data_processing/calculate_bike_per_day_nextbike.sql
# calculate station statistics for all providers (from station_movements)
psql -d shared_mobility -a -f data_processing/calculate_statistics_for_stations.sql
