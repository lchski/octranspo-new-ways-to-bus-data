# Data processing for [_NWTB Explorer_](https://nwtb-explorer.labs.lucascherkewski.com/)

This repository:

`load.sql` does the following:

1. loads OC Transpo’s GTFS data into a SQL database, using an export of the GTFS file from 2025-04-18 that included both the previous (“current”) and NWTB (“new”) schedules
2. filters the data to focus on six representative days (three for each schedule, see below)
3. adds or modifies additional fields, including `service_id` (service day) and `service_window` (based on OC Transpo’s [levels of service for the O-Train](https://www.octranspo.com/en/our-services/bus-o-train-network/service-types/o-train-line-1#hoursOp))
4. normalizes stop references to use the `stop_code` as the single common identifier instead of the `stop_id` (this reduces multi-platform stops, like Tunney’s Pasture and others on the Transitway, to a single stop entry, for ease of reference)
5. enhances the data with spatial details (currently just the ward number of each stop)

This results in, notably, these tables:

- `stops`: normalized stop info
- `trips`: all the trips occurring on the service dates
- `stop_times`: all the times buses or trains stop at `stops` for the `trips`

`output-for-web.sql` uses this data to output these files for the web:

- TKTK

## Representative days

The data loader uses the concept of a “service day” to capture three distinct levels of service in the OC Transpo schedule: weekday (Monday through Friday), Saturday, and Sunday. Six specific dates are used to filter service to represent these service days, comparing the previous and NWTB schedules:

| “Service day” | Previous schedule | NWTB schedule |
|---|---|---|
| Weekday | Friday, April 11 | Friday, May 9 |
| Saturday | April 12 | May 10 |
| Sunday | April 13 | May 11 |

I tried to pick these days to avoid holidays and special events that’d cause service changes.

_(NB! The weekday schedule, though based on a Friday, manually adds [a few additional trips](https://github.com/lchski/octranspo-new-ways-to-bus-data/blob/main/data/corrections/missing_shopper_trips.csv) for the [shopper routes](https://www.octranspo.com/en/our-services/bus-o-train-network/service-types/shopper-routes/), to ensure more accurate coverage in rural areas.)_
