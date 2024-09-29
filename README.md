## Loading

Requires `jq`. It’s good fun, I recommend it! (also `json2csv`)

Remove the JSON files in data/routes directory, then:

`./0-download-routes.sh`

```
rm 2-download-schedules.sh
./1-generate-schedule-download-curls.sh > 2-download-schedules.sh
```

Remove all the JSON files in the data/schedules directory, then:

```
chmod +x 2-download-schedules.sh
./2-download-schedules.sh
```

Remove the JSON files in data/stops, then:

```
./3-download-stops.sh
```

## Processing

Generate the feeder JSON / CSV:
```
node processing/index.js
node processing/routes-to-csv.js
```

If you're brave, load it directly into 
```

## Converting OCT GTFS to our GTFS subset

- `stop_times`: trip_id, arrival_time, stop_id, stop_sequence
  - stop_id: different from stop_code
- `stops`: stop_id, stop_code, stop_name, stop_lat, stop_lon, platform_code
  - stop_id: almost never the same between GTFS and NWTB (see, e.g., Pimisi and Tremblay entries, but likely for others too)
  - stop_code: 9 are null (they correspond to the ones from the NWTB data)
  - platform_code: these are embedded in the stop_name for the NWTB data
- `trips`: route_id, service_id, trip_id
  - (NOT INCLUDED) trip_headsign: could be nice for display info, e.g., the actual names of the various trips
  - (NOT INCLUDED) direction_id: again, could be nice

## Cleaning / merging

- `stops`
  - some stops (identified by a shared or similar `stop_name`) have different `stop_codes` between the two subsets (see query "all stop_codes that appear only once in the dataset"), see "SOMERSET W / PRESTON" as an easy example
    - consequences?
      - this means some stops from the NWTB won't have lat/lng available, unless we can map them to existing ones from the GTFS
    - fixes?
      - we could use DuckDB's similarity functions to approximate...
  - platform code
    - we could normalize `platform_code=1` to add "O-TRAIN WEST..." (etc for east), and merge in the code otherwise, to normalize
    - ooooor we could not, if we're just going to smush stop_codes into one mega stop (likely using the highest stop_times per stop_id as the name?)

## Preparing data for web

- for GTFS data, can "just" pick three representative days, and pull them based on schedules? [may require some fancy math for in / not in that day]


## API refs

for routes on today’s date:
curl 'https://plan.octranspo.com/plan/api/RouteTimetableAPI/GetRouteDirections?date=2024-8-13'

for routes on NWTB weekday:
curl 'https://plan.octranspo.com/plan/api/RouteTimetableAPI/GetRouteDirections?date=2025-9-15'

you can then use RouteDirectionKey (or Identifier and Direction, from within) from each entry to get the IDs to load into the RequestRouteTimetable functionv

simplest request for route data:
curl 'https://plan.octranspo.com/plan/RouteTimetable/RequestRouteTimetable' \
  -H 'content-type: application/json' \
  --data-raw '{"Date":"2025-9-15","RouteDirection":{"Key":{"Identifier":"502","Direction":"Direction1"}},"TimingPointsOnly":false}' 
