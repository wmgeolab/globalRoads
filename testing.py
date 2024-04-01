import requests

from_lat = "80.58717891134381"
from_lon = "30.467257838998556"

to_lat = "80.2354125167115"
to_lon = "30.184988924925666"



url = "http://osrm:80/route/v1/driving/" + str(from_lat) + "," + str(from_lon) + ";" + str(to_lat) + "," + str(to_lon) + "?overview=false&steps=true"
query = osm_request(url, RETRIES, RESPONSEWAIT)
tot = 0
dur = 0
for i in query["routes"][0]["legs"][0]["steps"]:
    print(str(i["name"]) + ", " + str(i["distance"]) + "," + str(i["maneuver"]["type"]))
    tot = tot + float(i["distance"])
    dur = dur + float(i["duration"])

print(tot)



duration = query["routes"][0]["duration"]
distance = query["routes"][0]["distance"]