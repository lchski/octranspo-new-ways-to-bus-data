curl 'https://plan.octranspo.com/plan/api/StopsAPI/GetStopsInMapBounds?northEastBoundLatitude=45.597634&northEastBoundLongitude=-75.020160&southWestBoundLatitude=45.075482&southWestBoundLongitude=-76.130916' | jq > 'data/stops/stops-raw.json'

# convert downloaded data to GTFS schema subset that we use
cat data/stops/stops-raw.json | jq 'map({
    stop_id: .Stop.Identifier,
    stop_code: .Stop.PhoneNumber,
    stop_name: .Stop.Description,
    stop_lat: .Stop.Location.LongLat.Latitude,
    stop_lon: .Stop.Location.LongLat.Longitude
})' > data/out/gtfs-stops-entire.json

cat data/out/gtfs-stops-entire.json | json2csv > data/out/gtfs-stops-entire.csv
