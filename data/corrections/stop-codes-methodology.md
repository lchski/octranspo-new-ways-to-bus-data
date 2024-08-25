e.g., octranspo-gtfs ,7383    ,          ,ALTA VISTA / AYERS                ,45.376542 ,-75.660768 ,

- look for stops with a similar stop name:
	`from stops where stop_name like 'ALTA VISTA / A%';`
- if none immediately apparent, look at the lat / lng in Google Maps
	- sometimes there's an actual stop there, with a code—ideal!
	- in this case, the lat / lng returns further up the road, basically at ALTA VISTA / RIDGEMONT
	- the last time a stop was there looks like... 2009?!
- figure out how many trips this impacts:
	- stop_times: `from stop_times where stop_id = '7383';`
	- trips: `from trips where trip_id in (select trip_id from stop_times where stop_id = '7383');`
	- "journeys" (i.e., sequence of stops): `from stop_times where trip_id in (select trip_id from stop_times where stop_id = '7383');`
		add `and stop_sequence < 4` to just get the first three stops for each trip
	- NB: if the stop_sequence for all trips is 1, it's probably a lay-up / starting / placeholder stop
- look up the route and stop sequence in the travel planner (pick the right NWTB / GTFS and date)
	- if blank code in middle of common journey, likely a temporary stop—check the route in Alerts: https://www.octranspo.com/en/alerts/
	- if blank code at start of journey, followed by same / similar stop very nearby, likely a layup
- if apparent temp:
	code TEMP
- if apparent layup:
	code LAYUP and note the corresponding stop detail

```sql
CREATE MACRO similar_stop_names(stop_name_param) AS TABLE
	SELECT * FROM stops WHERE stop_name LIKE stop_name_param;

CREATE MACRO stop_times_for_stop_id(stop_id_param) AS TABLE
	select * from stop_times where stop_id = stop_id_param::VARCHAR;

CREATE MACRO trips_for_stop_id(stop_id_param) AS TABLE
	select * from trips where trip_id in (select trip_id from stop_times where stop_id = stop_id_param::VARCHAR);

CREATE MACRO journeys_for_stop_id(stop_id_param, n_stops := 3) AS TABLE
	select st.*, s.stop_name from stop_times st LEFT JOIN stops s ON st.source = s.source AND st.stop_id = s.stop_id where trip_id in (select trip_id from stop_times where stop_id = stop_id_param::VARCHAR) and stop_sequence < (n_stops + 1);
```

run this (replace stop name / stop ID as required):

```sql
FROM similar_stop_names('DAUPHIN%');

FROM stop_times_for_stop_id('10283');

FROM trips_for_stop_id('10283');

FROM journeys_for_stop_id('10283');

-- optional, for more journeys, note particular syntax
FROM journeys_for_stop_id('10283', n_stops := 5);

-- optional, for custom journey sequencing (i.e., mid-route stops) based on stop_times
select * from stop_times where trip_id in (select trip_id from stop_times where stop_id = '10283') and stop_sequence > 60 and stop_sequence < 66;
```