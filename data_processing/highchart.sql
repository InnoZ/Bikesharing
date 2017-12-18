-- calculate vehicle counts, sightings and movement rates for each city and provider
DROP TABLE IF EXISTS bikesharing.vehicle_sightings_highchart;
CREATE TABLE bikesharing.vehicle_sightings_highchart AS
SELECT provider, city, year, month, week, doy, count(*), sum(count), sum(count)/count(*) AS movements_per_vhc
FROM
    (SELECT provider, city, key, date_part('month', first_seen_at) AS month,  date_part('year', first_seen_at) AS year,  date_part('week', first_seen_at) AS week,  date_part('doy', first_seen_at) AS doy, count(*)
    FROM bikesharing.vehicle_sightings
    GROUP BY provider, city, key, date_part('month', first_seen_at),  date_part('year', first_seen_at), date_part('week', first_seen_at),  date_part('doy', first_seen_at)) t1
GROUP BY provider, city, month, year, week, doy
ORDER BY year ASC, doy ASC, city ASC, provider ASC;

-- calculate vehicle counts, sightings and movement rates for each city and provider
DROP TABLE IF EXISTS bikesharing.station_movements_highchart;
CREATE TABLE bikesharing.station_movements_highchart AS
SELECT provider, city, year, month, count(*) AS stations, sum(count), sum(count)/count(*) AS movements_per_station
FROM
    (SELECT provider, city, key, date_part('month', seen_at) AS month,  date_part('year', seen_at) AS year,  count(*)
    FROM bikesharing.station_movements WHERE city IN ('berlin', 'münchen', 'mainz', 'london', 'hamburg', 'köln', 'frankfurt', 'paris', 'barcelona', 'new york, ny', 'washingtondc', 'montreal, qc', 'san francisco bay area, ca')
    GROUP BY provider, city, key, date_part('year', seen_at), date_part('month', seen_at)) t1
GROUP BY provider, city, month, year
ORDER BY year ASC, month ASC, city ASC, provider ASC;
