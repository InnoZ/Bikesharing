SELECT date_part('dow', first_seen_at) AS dow, 
  date_part('hour', first_seen_at) AS hour,
  count(*)
FROM bikesharing.vehicle_sightings_year
GROUP BY date_part('dow', first_seen_at), date_part('hour', first_seen_at)
ORDER BY dow ASC, hour ASC
