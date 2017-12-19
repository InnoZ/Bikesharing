#!/bin/bash

psql -d shared_mobility -a -f /data_processing/calculate_additional_values.sql
