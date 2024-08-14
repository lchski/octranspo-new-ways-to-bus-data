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

const scheduleData = schedules.map(schedule => {
	const scheduleDom = parseDomFromFile(schedule.path)

	return {
		...schedule,
		timetable: extractTimetableFromDom(scheduleDom),
		tripIds: extractTripIdsFromDom(scheduleDom)
	}
})

fs.writeFileSync('data/out/schedules.json', JSON.stringify(scheduleData))

const end = Date.now()

console.log("processing began and ended at", [start, end])
