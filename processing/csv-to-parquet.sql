CREATE TABLE stops as SELECT * FROM read_csv("data/out/gtfs-stops.csv");
CREATE TABLE stop_times as SELECT * FROM read_csv("data/out/gtfs-stop-times.csv", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});
CREATE TABLE trips as SELECT * FROM read_csv("data/out/gtfs-trips.csv");

EXPORT DATABASE 'data/out/parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');
