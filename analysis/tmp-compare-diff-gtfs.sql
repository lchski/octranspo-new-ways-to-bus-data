CREATE TABLE stops as
	SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon
	FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/stops.txt");
CREATE TABLE stop_times as
	SELECT trip_id, arrival_time, stop_id, stop_sequence
	FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/stop_times.txt", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});
CREATE TABLE trips as
	SELECT route_id, service_id, trip_id, trip_headsign, direction_id -- trip_headsign probably not useful? but maybe if we're trying to join / infer directions later
	FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/trips.txt");


-- CREATE TABLE feed_info as SELECT * FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/feed_info.txt", dateformat='%Y%m%d', types={'feed_start_date': 'DATE', 'feed_end_date': 'DATE'});
CREATE TABLE calendar as SELECT * FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/calendar.txt", dateformat='%Y%m%d', types={'start_date': 'DATE', 'end_date': 'DATE'});
CREATE TABLE calendar_dates as SELECT * FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/calendar_dates.txt", dateformat='%Y%m%d', types={'date': 'DATE'});

ALTER TABLE stops ADD COLUMN source VARCHAR DEFAULT 'octranspo-legacy-gtfs';
ALTER TABLE stops ALTER COLUMN source DROP DEFAULT;

ALTER TABLE stop_times ADD COLUMN source VARCHAR DEFAULT 'octranspo-legacy-gtfs';
ALTER TABLE stop_times ALTER COLUMN source DROP DEFAULT;

ALTER TABLE trips ADD COLUMN source VARCHAR DEFAULT 'octranspo-legacy-gtfs';
ALTER TABLE trips ALTER COLUMN source DROP DEFAULT;

...

CREATE TEMP TABLE gtfs_representative_services AS
    SELECT *
    FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-legacy-gtfs_representative_services.csv");

...






CREATE TABLE stops as
	SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon, platform_code
	FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/stops.txt");
CREATE TABLE stop_times as
	SELECT trip_id, arrival_time, stop_id, stop_sequence
	FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/stop_times.txt", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});
CREATE TABLE trips as
	SELECT route_id, service_id, trip_id, trip_headsign, direction_id -- trip_headsign probably not useful? but maybe if we're trying to join / infer directions later
	FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/trips.txt");


CREATE TABLE feed_info as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/feed_info.txt", dateformat='%Y%m%d', types={'feed_start_date': 'DATE', 'feed_end_date': 'DATE'});
CREATE TABLE calendar as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/calendar.txt", dateformat='%Y%m%d', types={'start_date': 'DATE', 'end_date': 'DATE'});
CREATE TABLE calendar_dates as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/calendar_dates.txt", dateformat='%Y%m%d', types={'date': 'DATE'});

...

CREATE TEMP TABLE gtfs_representative_services AS
    SELECT *
    FROM read_csv("data/source/octranspo-gtfs/2024-08-26-gtfs_representative_services.csv");

...
