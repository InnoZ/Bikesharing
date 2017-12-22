SELECT date_part('dow', started_at) AS dow,
  date_part('hour', started_at) AS hour,
  count(*)
FROM bikesharing.vehicle_movements_extended
GROUP BY date_part('dow', started_at), date_part('hour', started_at)
ORDER BY dow ASC, hour ASC
