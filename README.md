## Loading

Requires `jq`. It’s good fun, I recommend it!

`download-routes.sh`

run `./1-generate-schedule-download-curls.sh > 2-download-schedules.sh` 

run ```
chmod+x 2-download-schedules.sh
./2-download-schedules.sh
```

## Processing

`node processing/index.js`

`duckdb < processing/csv-to-parquet.sql`


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
