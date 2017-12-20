require 'pg'

db = 'pgrouting_bikesharing'
conn = PG::Connection.open(dbname: db)
daytimes = %w[morning evening]
schema = 'berlin'
node_schema = 'berlin_roads_topo'
poi = 'ostkreuz'

# has to contain columns for start & end of the booking e.g. start_geom and end_geom
# Get the start nodes

daytimes.each do |daytime|
  table = "#{schema}.bikesharing_trips_#{daytime}_#{poi}"
  conn.exec("DROP TABLE IF EXISTS #{schema}.start_point_node_#{daytime};")
  conn.exec("CREATE TABLE #{schema}.start_point_node_#{daytime}(gid INTEGER, node_id INTEGER);")
  # get the count of existing rows in the table
  count = conn.exec("SELECT count(*) FROM #{schema}.bikesharing_trips_#{daytime}_#{poi}").values.first.first.to_i
  puts "Count: #{count}"
  puts "Calculate Start Nodes #{daytime}"
  (1..count).each do |i|
    conn.exec("DROP TABLE IF EXISTS #{schema}.start_point_node_temp_#{daytime};")
    conn.exec("CREATE TABLE #{schema}.start_point_node_temp_#{daytime} AS SELECT #{table}.route_id, node.node_id, #{table}.start_station_name  FROM #{table}, #{node_schema}.node
             WHERE #{table}.start_geom && ST_Expand(node.geom, 100) AND #{table}.route_id = #{i}
             ORDER BY ST_Distance(#{table}.start_geom, node.geom) ASC Limit 1;")
    conn.exec("INSERT INTO #{schema}.start_point_node_#{daytime} SELECT #{schema}.start_point_node_temp_#{daytime}.route_id, #{schema}.start_point_node_temp_#{daytime}.node_id FROM
             #{schema}.start_point_node_temp_#{daytime};")
  end
end
Kernel.sleep(5)


# Get the end node

daytimes.each do |daytime|
  table = "#{schema}.bikesharing_trips_#{daytime}_#{poi}"
  conn.exec("DROP TABLE IF EXISTS #{schema}.end_point_node_#{daytime};")
  conn.exec("CREATE TABLE #{schema}.end_point_node_#{daytime}(gid INTEGER, node_id INTEGER);")
  # get the count of existing rows in the table
  count = conn.exec("SELECT count(*) FROM #{schema}.bikesharing_trips_#{daytime}_#{poi}").values.first.first.to_i
  puts "Count: #{count}"
  puts "Calculate End Nodes #{daytime}"
  (1..count).each do |i|
    conn.exec("DROP TABLE IF EXISTS #{schema}.end_point_node_temp_#{daytime};")
    conn.exec("CREATE TABLE #{schema}.end_point_node_temp_#{daytime} AS SELECT #{table}.route_id, node.node_id, #{table}.start_station_name  FROM #{table}, #{node_schema}.node
             WHERE #{table}.end_geom && ST_Expand(node.geom, 100) AND #{table}.route_id = #{i}
             ORDER BY ST_Distance(#{table}.end_geom, node.geom) ASC Limit 1;")
    conn.exec("INSERT INTO #{schema}.end_point_node_#{daytime} SELECT #{schema}.end_point_node_temp_#{daytime}.route_id, #{schema}.end_point_node_temp_#{daytime}.node_id FROM
             #{schema}.end_point_node_temp_#{daytime};")
  end
end
Kernel.sleep(5)


# Get the shortest path
# between start and end node
daytimes.each do |daytime|
  table = "#{schema}.bikesharing_trips_#{daytime}_#{poi}"
  conn.exec("DROP TABLE IF EXISTS #{schema}.routing_start_point_#{daytime};")
  conn.exec("CREATE TABLE #{schema}.routing_start_point_#{daytime}(geom GEOMETRY);")
  count = conn.exec("SELECT count(*) FROM #{schema}.bikesharing_trips_#{daytime}_#{poi}").values.first.first.to_i
  puts "Count: #{count}"
  puts "Calculate Shortest Distance #{daytime}"

  (1..count).each do |i|
    conn.exec("DROP TABLE IF EXISTS #{schema}.routing_start_point_temp_#{daytime};")
    conn.exec"CREATE TABLE #{schema}.routing_start_point_temp_#{daytime} AS SELECT * FROM #{node_schema}.edge_data JOIN
             (SELECT * FROM pgr_dijkstra ('SELECT edge_id AS id, start_node::int4 AS source, end_node::int4 AS target, shape_leng::float8 AS cost FROM #{node_schema}.edge_data',
             (SELECT node_id FROM #{schema}.start_point_node_#{daytime} WHERE gid = #{i}),
             (SELECT node_id FROM #{schema}.end_point_node_#{daytime} WHERE gid = #{i}), FALSE, FALSE))
             AS route ON #{node_schema}.edge_data.edge_id = route.id2 ORDER BY seq;"
    conn.exec("INSERT INTO #{schema}.routing_start_point_#{daytime} SELECT #{schema}.routing_start_point_temp_#{daytime}.geom as geom from #{schema}.routing_start_point_temp_#{daytime};")
  end
  conn.exec("DROP TABLE IF EXISTS #{schema}.routing_start_point_count_#{daytime};")
  conn.exec("CREATE TABLE #{schema}.routing_start_point_count_#{daytime} AS SELECT geom, count(*) FROM #{schema}.routing_start_point_#{daytime} GROUP BY geom;")
end
conn.close
puts 'Done!'
