# Data processing for [_NWTB Explorer_](https://nwtb-explorer.labs.lucascherkewski.com/)

This repository converts data in [OC Transpo’s GTFS file](https://www.octranspo.com/en/plan-your-trip/travel-tools/developers/) into a format more conducive to analysis, and outputs it for use in the NWTB Explorer.

It uses an export of the GTFS file from 2025-04-18 that included both the previous and NWTB schedules. (If you’d like to run the repository yourself, [contact me](https://lucascherkewski.com/contact/) for a copy of the GTFS export!)

***

`load.sql` does the following:

1. loads OC Transpo’s GTFS data into a SQL database
2. filters the data to focus on six representative days (three for each schedule, see below), adding a `source` field to capture the schedule variant (“legacy” for the previous schedule, and “nwtb” for the NWTB schedule)
3. adds or modifies additional fields, including `service_id` (service day) and `service_window` (based on OC Transpo’s [levels of service for the O-Train](https://www.octranspo.com/en/our-services/bus-o-train-network/service-types/o-train-line-1#hoursOp))
4. normalizes stop references to use the `stop_code` as the single common identifier instead of the `stop_id` (this reduces multi-platform stops, like Tunney’s Pasture and others on the Transitway, to a single stop entry, for ease of reference)
5. enhances the data with spatial details (currently just the ward number of each stop)

This results in, notably, these tables:

- `stop_normalized`: normalized stop info
- `trips`: all the trips occurring on the service dates
- `stop_times`: all the times buses or trains stop at `stops` for the `trips`

`output-for-web.sql` uses this data to create these temporary tables, saved to the `data/out/for-web/` directory as compressed Parquet files for use in the Explorer:

- `web_stop_times_by_stop`: # of times buses / trains arrive (“stop”) at a stop, rolled up by `source`, `service_id`, `service_window`, and `stop_code`
- `web_stop_times`: raw data for all bus / train arrivals, including “time to next arrival” (for a combination of `stop_code`, `source`, `service_id`, `route_id`, `direction_id` to make sure it’s an equivalent bus / train)
- `routes`: summary data on routes, including the most common headsign (direction label, like “Tunney’s Pasture” or “Blair” for O-Train Line 1) and number of trips

The `stops_normalized` table is also output for web, as `stops_normalized.parquet`.

_Note: When output for web, the `source` field is remapped to “current” (for the “legacy” or previous schedule) and “new” (for the NWTB schedule)._

## Representative days

The data loader uses the concept of a “service day” to capture three distinct levels of service in the OC Transpo schedule: weekday (Monday through Friday), Saturday, and Sunday. Six specific dates are used to filter service to represent these service days, comparing the previous and NWTB schedules:

| “Service day” | Previous schedule | NWTB schedule |
|---|---|---|
| Weekday | Friday, April 11 | Friday, May 9 |
| Saturday | April 12 | May 10 |
| Sunday | April 13 | May 11 |

I tried to pick these days to avoid holidays and special events that’d cause service changes.

_(NB! The weekday schedule, though based on a Friday, manually adds [a few additional trips](https://github.com/lchski/octranspo-new-ways-to-bus-data/blob/main/data/corrections/missing_shopper_trips.csv) for the [shopper routes](https://www.octranspo.com/en/our-services/bus-o-train-network/service-types/shopper-routes/), to ensure more accurate coverage in rural areas.)_
