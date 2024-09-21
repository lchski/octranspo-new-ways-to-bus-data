curl 'https://plan.octranspo.com/plan/api/StopsAPI/GetStopsInMapBounds?northEastBoundLatitude=45.597634&northEastBoundLongitude=-75.020160&southWestBoundLatitude=45.075482&southWestBoundLongitude=-76.130916' | jq > 'data/stops/stops-raw.json'

# to extract the right info into our stop_code schema, use this on the downloaded JSON
jq '.[] | {
    stop_id: .Stop.Identifier,
    stop_code: .Stop.PhoneNumber,
    stop_name: .Stop.Description,
    stop_lat: .Stop.Location.LongLat.Latitude,
    stop_lon: .Stop.Location.LongLat.Longitude
}'
