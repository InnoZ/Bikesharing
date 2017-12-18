WITH total_bikes_per_observation AS (
  SELECT
  date_trunc('minute', seen_at) AS seen_at,
  sum(vehicles_available) AS vehicles,
  sum(CASE WHEN vehicles_available=5 THEN 1 ELSE 0 END) AS plus5_stations,
  sum(CASE WHEN vehicles_available BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS plus0_stations,
  sum(CASE WHEN vehicles_available = 0 THEN 1 ELSE 0 END) AS empty_stations
  FROM
  duesseldorf.station_occupancies
  WHERE vehicle_type='bike'
  GROUP BY date_trunc('minute', seen_at)
)

SELECT
  date_trunc('day', seen_at) AS seen_on,
  max(vehicles) AS vehicles,
  avg(plus5_stations) AS avg_plus5_stations,
  max(plus5_stations) AS max_plus5_stations,
  min(plus5_stations) AS max_plus5_stations,
  avg(plus0_stations) AS avg_plus0_stations,
  max(plus0_stations) AS max_plus0_stations,
  min(plus0_stations) AS max_plus0_stations,
  avg(empty_stations) AS avg_0_stations,
  max(empty_stations) AS max_0_stations,
  min(empty_stations) AS max_0_stations
FROM total_bikes_per_observation
GROUP BY date_trunc('day', seen_at)
ORDER BY date_trunc('day', seen_at)
