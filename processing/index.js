import fs from 'fs'
import { parseFromString } from 'dom-parser'

const start = Date.now()

console.log("processing began at", start)

const parseDomFromFile = (path) => {
	const fileJson = JSON.parse(fs.readFileSync(path))

	return parseFromString(fileJson.Html)
}

const extractTimetableFromDom = (dom) => JSON.parse(
	dom.getElementById('RouteTimetableContainer')
		.getAttribute('data-route-timetable-view-model')
		.replaceAll('&quot;', '"')
)

const extractTripIdsFromDom = (dom) => {
	const scriptTags = dom.getElementsByTagName('script')

	const hastinfoTag = scriptTags[1] // TODO: build a check in for this?

	const tripIdsRegex = /hastinfo.routeTimetable.tripsKeys = (\[(?:"[0-9_]*?",?)*\])/;
	
	return JSON.parse(hastinfoTag.innerHTML.match(tripIdsRegex)[1])
}

const schedules = fs.readdirSync('data/schedules/', {
		withFileTypes: true,
		recursive: true
	})
	.filter(item => ! item.isDirectory())
	.filter(item => item.name.includes('.json'))
	.map(item => ({
		schedule_id: item.name,
		service: item.path.split('/')[2],
		path: item.path + '/' + item.name
	}))

let testSchedules = schedules.slice(0,4)

const processStopsFromTimetable = (timetable) => timetable.StopViewModels.map((stop, i) => ({
	stop_index: i,
	name: stop.DisplayText.split(' - ', 2)[0], // TODO / NB: this may cause a bug if " - " is found in a stop name when not separating the stop label and code
	code: stop.DisplayText
		.split(' - ', 2)[1]
		.replaceAll('(', '')
		.replaceAll(')', ''),
	id: stop.Identifier
}))

const scheduleData = testSchedules.map(schedule => {
	const scheduleDom = parseDomFromFile(schedule.path)
	
	const timetable = extractTimetableFromDom(scheduleDom)
	const stops = processStopsFromTimetable(timetable)

	return {
		...schedule,
		stops: stops,
		stop_times: Object.values(timetable.PassingTimeViewModelsByStops).map((stop_time, i) => ({
			stop_index: i,
			stop_code: stops[i].code,
			time: stop_time.Time // TODO: figure out why this isn't working :)
		})),
		timetableTripCount: timetable.TripsCount,
		tripIds: extractTripIdsFromDom(scheduleDom)
	}
})

fs.writeFileSync('data/out/schedules.json', JSON.stringify(scheduleData))

const end = Date.now()

console.log("processing began and ended at", [start, end])
