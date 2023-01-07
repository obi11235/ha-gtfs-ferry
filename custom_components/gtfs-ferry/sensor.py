from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone, timedelta
from dateutil import parser
from zoneinfo import ZoneInfo
from zipfile import ZipFile
from csv import DictReader
from copy import deepcopy
import requests
import io

import logging
import voluptuous as vol

from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import (CONF_NAME)
import homeassistant.util.dt as dt_util
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle
import homeassistant.helpers.config_validation as cv

_LOGGER = logging.getLogger(__name__)

CONF_FERRY_ROUTES_URL = 'ferry_routes_url'
CONF_FERRY_TRIPS_URL = 'ferry_tripS_url'
CONF_ICON = 'icon'

CONF_DEPARTURES = 'departures'
CONF_ROUTE_ID = 'route_id'
CONF_DIRECTION_ID = 'direction_id'
CONF_STOP_ID = 'stop_id'

DEFAULT_ICON = 'mdi:ferry'

# ATTR_NUM_BIKES_AVAILABLE = 'num bikes available'

MIN_TIME_BETWEEN_UPDATES = timedelta(seconds=60)
TIME_STR_FORMAT = "%H:%M"

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_FERRY_ROUTES_URL): cv.string,
    vol.Optional(CONF_FERRY_TRIPS_URL, default=None): cv.string,
    vol.Optional(CONF_ICON, default=DEFAULT_ICON): cv.string,

    vol.Required(CONF_DEPARTURES): [{
        vol.Required(CONF_NAME): cv.string,
        vol.Required(CONF_ROUTE_ID): cv.string,
        vol.Required(CONF_DIRECTION_ID): cv.string,
        vol.Required(CONF_STOP_ID): cv.string,

    }]
})

def due_in_minutes(timestamp):
    """Get the remaining minutes from now until a given datetime object."""
    diff = timestamp - dt_util.utcnow().replace(tzinfo=None)
    return int(diff.total_seconds() / 60)

def setup_platform(hass, config, add_devices, discovery_info=None):
    data = GTFSFerry(dt_util.DEFAULT_TIME_ZONE, config.get(CONF_FERRY_ROUTES_URL), config.get(CONF_FERRY_TRIPS_URL))
    sensors = []
    for departures in config.get(CONF_DEPARTURES):
        sensors.append(GTFSFerrySensor(
            data,
            departures.get(CONF_NAME),
            departures.get(CONF_ROUTE_ID),
            departures.get(CONF_DIRECTION_ID),
            departures.get(CONF_STOP_ID),
            config.get(CONF_ICON)
        ))

    add_devices(sensors)

class GTFSFerrySensor(Entity):

    def __init__(self, data, name, route_id, direction_id, stop_id, icon):
        """Initialize the sensor."""
        self.data = data
        self._name = name
        self._route_id = route_id
        self._direction_id = direction_id
        self._stop_id = stop_id
        self._icon = icon
        self._current_data = []
        self.update()

    @property
    def name(self):
        return self._name

    @property
    def state(self):
        """Return the state of the sensor."""
        return due_in_minutes(datetime.combine(self._current_data[0].date, self._current_data[0].departure_time))

    @property
    def extra_state_attributes(self):
        """Return the state attributes."""
        # attrs = {
        #     ATTR_STATION_NAME: self.data.info[self._station_id].name,
        # }
        attrs = {}
        return attrs

    @property
    def unit_of_measurement(self):
        """Return the unit this state is expressed in."""
        return "min"

    @property
    def icon(self):
        return self._icon

    def update(self):
        """Get the latest data and update the states."""
        self.data.update(3600, 60)

        self._current_data = self.data.get_remaining_stops(self._route_id, self._direction_id, self._stop_id)

        _LOGGER.debug("Sensor Update:")
        _LOGGER.debug("...Name: {0}".format(self._name))


def download_extract_zip(url):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    response = requests.get(url)
    with ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile

class StopEntity():
    def __init__(self, trip_id, arrival_time, departure_time, stop_id, stop_sequence):
        self.trip_id = trip_id
        self.arrival_time = arrival_time
        self.departure_time = departure_time
        self.stop_id = stop_id
        self.stop_sequence = stop_sequence
        self.arrival_time_actual = None
        self.departure_time_actual = None
        self.date = None

    def __str__(self):
        return str(self.__dict__)

class TripEntity():
    def __init__(self, trip_id, route_id, service_id, direction_id):
        self.trip_id = trip_id
        self.service_id = service_id
        self.route_id = route_id
        self.direction_id = direction_id

    def __str__(self):
        return str(self.__dict__)


class GTFSFerry():

    def __init__(self, timezone, routes_url, trip_url = None):
        self.timezone = timezone
        self.routes_url = routes_url
        self.trip_url = trip_url
        self.trips = []
        self.stops = {}
        self.tomorrow_service_id = None
        self.today_service_id = None
        self.last_static_update = None
        self.last_rt_update = None

        self.update_static_data()
        self.update_realtime_data()

    def update_static_data(self):

        #### Prep data to be updated hourly ####
        services = []
        exceptions = []

        for filename, data in download_extract_zip(self.routes_url):
            if filename == 'calendar.txt':
                reader = DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'))
                for row in reader:
                    services.append(row)

            if filename == 'calendar_dates.txt':
                reader = DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'))
                for row in reader:
                    exceptions.append(row)

            if filename == 'trips.txt':
                reader = DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'))
                for row in reader:
                    # if row['route_id'] == route_id and row['direction_id'] == direction_id:
                        self.trips.append(TripEntity(row['trip_id'], row['route_id'], row['service_id'], row['direction_id']))

            if filename == 'stop_times.txt':
                reader = DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'))
                for row in reader:
                    # if row['stop_id'] in stop_ids:
                        cur_stop = StopEntity(row['trip_id'], parser.parse(row['arrival_time']).replace(tzinfo=ZoneInfo("US/Eastern")).time(), parser.parse(row['departure_time']).replace(tzinfo=ZoneInfo("US/Eastern")).time(), row['stop_id'], row['stop_sequence'])
                        if cur_stop.trip_id not in self.stops:
                            self.stops[cur_stop.trip_id] = {}
                        self.stops[cur_stop.trip_id][cur_stop.stop_sequence] = cur_stop

            # if filename == 'stops.txt':
            #     reader = DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'))
            #     for row in reader:
            #         if row['stop_id'] in stop_ids:
            #             stop = row
            # if filename == 'routes.txt':
            #     reader = DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'))
            #     for row in reader:
            #         print(row)


        now_local = datetime.now(self.timezone)
        tomorrow_local = (now_local + timedelta(1)).replace(hour=0, minute=0, second=0)

        #Figure out today and tomorrows service_id
        days = {'0':'monday','1':'tuesday','2':'wednesday','3':'thursday','4':'friday','5':'saturday','6':'sunday'}

        self.today_service_id = None
        self.tomorrow_service_id = None
        for service in services:
            if service['start_date'] == service['end_date']:
                if parser.parse(service['start_date']).date() == now_local.date() and service[days[str(now_local.weekday())]] == '1':
                    self.today_service_id = service['service_id']
                if parser.parse(service['start_date']).date() == tomorrow_local.date() and service[days[str(tomorrow_local.weekday())]] == '1':
                    self.tomorrow_service_id = service['service_id']
            else:
                if parser.parse(service['start_date']).date() <= now_local.date() and parser.parse(service['end_date']).date() >= now_local.date() and service[days[str(now_local.weekday())]] == '1':
                    self.today_service_id = service['service_id']
                if parser.parse(service['start_date']).date() <= tomorrow_local.date() and parser.parse(service['end_date']).date() >= tomorrow_local.date() and service[days[str(tomorrow_local.weekday())]] == '1':
                    self.tomorrow_service_id = service['service_id']

        for exception in exceptions:
            if parser.parse(exception['date']).date() == now_local.date():
                if exception['exception_type'] == '1':
                    self.today_service_id = exception['service_id']
                elif exception['exception_type'] == '2' and self.today_service_id == exception['service_id']:
                    self.today_service_id = None

            if parser.parse(exception['date']).date() == tomorrow_local.date():
                if exception['exception_type'] == '1':
                    self.tomorrow_service_id = exception['service_id']
                elif exception['exception_type'] == '2' and self.tomorrow_service_id == exception['service_id']:
                    self.tomorrow_service_id = None

        self.last_static_update = datetime.now()

    def update_realtime_data(self):

        ##### Update Real time data #####
        if self.trip_url != None:
            #purge old real time data
            for route in self.stops:
                for stop_sequence in self.stops[route]:
                    stop = self.stops[route][stop_sequence]
                    stop.departure_time_actual = None
                    stop.arrival_time_actual = None

            #Get realtme data
            feed = gtfs_realtime_pb2.FeedMessage()
            response = requests.get(self.trip_url)
            if response.status_code != 200:
                print("updating route status got {}:{}".format(response.status_code,response.content))
                exit()

            feed.ParseFromString(response.content)
            departure_times = {}

            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    for stop in entity.trip_update.stop_time_update:
                        if entity.trip_update.trip.trip_id in self.stops and stop.stop_sequence in self.stops[entity.trip_update.trip.trip_id]:
                            self.stops[entity.trip_update.trip.trip_id][stop.stop_sequence].arrival_time_actual = datetime.fromtimestamp(stop.arrival.time).replace(tzinfo=ZoneInfo("US/Eastern")).time()
                            self.stops[entity.trip_update.trip.trip_id][stop.stop_sequence].departure_time_actual = datetime.fromtimestamp(stop.departure.time).replace(tzinfo=ZoneInfo("US/Eastern")).time()

        self.last_rt_update = datetime.now()
    
    def get_remaining_stops(self, route_id, direction_id, stop_id):
        stops_remaining = []
        now_local = datetime.now(self.timezone)
        tomorrow_local = (now_local + timedelta(1)).replace(hour=0, minute=0, second=0)

        for trip in self.trips:
            if trip.route_id == route_id and trip.direction_id == direction_id:
                for stop_sequence in self.stops[trip.trip_id]:
                    stop = self.stops[trip.trip_id][stop_sequence]
                    if stop.stop_id == stop_id:
                        if stop.departure_time > now_local.time() and trip.service_id == self.today_service_id:
                            cur = deepcopy(stop)
                            cur.date = now_local.date()
                            stops_remaining.append(cur)

                        if stop.departure_time > tomorrow_local.time() and trip.service_id == self.tomorrow_service_id:
                            cur = deepcopy(stop)
                            cur.date = tomorrow_local.date()
                            stops_remaining.append(cur)

        stops_remaining.sort(key=lambda t: datetime.combine(t.date, t.departure_time))

        return stops_remaining

    def update(self, static_update_sec, rt_update_sec):
        if (datetime.now() - self.last_static_update).total_seconds() > static_update_sec:
            self.update_static_data()

        if (datetime.now() - self.last_rt_update).total_seconds() > rt_update_sec:
            self.update_realtime_data()
