CREATE TABLE feed_info as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-09-GTFSExport/feed_info.txt", dateformat='%Y%m%d', types={'feed_start_date': 'DATE', 'feed_end_date': 'DATE'});
CREATE TABLE calendar as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-09-GTFSExport/calendar.txt", dateformat='%Y%m%d', types={'start_date': 'DATE', 'end_date': 'DATE'});
CREATE TABLE calendar_dates as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-09-GTFSExport/calendar_dates.txt", dateformat='%Y%m%d', types={'date': 'DATE'});

CREATE TABLE stops as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-09-GTFSExport/stops.txt");
CREATE TABLE stop_times as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-09-GTFSExport/stop_times.txt", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});
CREATE TABLE trips as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-09-GTFSExport/trips.txt");

-- add OCT GTFS as source
ALTER TABLE stops ADD COLUMN source VARCHAR DEFAULT 'octranspo-gtfs';
ALTER TABLE stops ALTER COLUMN source DROP DEFAULT;

ALTER TABLE stop_times ADD COLUMN source VARCHAR DEFAULT 'octranspo-gtfs';
ALTER TABLE stop_times ALTER COLUMN source DROP DEFAULT;

ALTER TABLE trips ADD COLUMN source VARCHAR DEFAULT 'octranspo-gtfs';
ALTER TABLE trips ALTER COLUMN source DROP DEFAULT;

-- load the NWTB data
INSERT INTO
	stops (stop_code, stop_id, stop_name)
	SELECT * FROM read_csv("data/out/gtfs-stops.csv");

INSERT INTO
	stop_times (trip_id, arrival_time, stop_id, stop_sequence)
	SELECT * EXCLUDE (stop_code) FROM read_csv("data/out/gtfs-stop-times.csv");

INSERT INTO
	trips (route_id, service_id, trip_id)
	SELECT * FROM read_csv("data/out/gtfs-trips.csv");

-- add NWTB as source
UPDATE stops
	SET source = 'nwtb'
	WHERE source IS NULL;

UPDATE stop_times
	SET source = 'nwtb'
	WHERE source IS NULL;

UPDATE trips
	SET source = 'nwtb'
	WHERE source IS NULL;


-- TODO: join stop_code into stop_times? we do this already in the NWTB data, would just have to always join on source and the var of interest
--		 NB: we'd still have the blank stop codes for a few

EXPORT DATABASE 'oc_transpo_gtfs' (FORMAT 'parquet', COMPRESSION 'GZIP');
