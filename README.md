# Home Assistant


inspired by https://github.com/mark1foley/ha-gtfs-rt-v2


Example for NYC Ferry
```
sensor:
  - platform: gtfs_ferry
    ferry_routes_url: 'http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx'
    ferry_trips_url: 'http://nycferry.connexionz.net/rtt/public/utility/gtfsrealtime.aspx/tripupdate'
    departures: 
      - name: "Ferry Wall AS"
        route_id: 'AS'
        direction_id: '1'
        stop_id: '87'
      - name: "Ferry Wall ER"
        route_id: 'ER'
        direction_id: '1'
        stop_id: '87'

```
