# globalRoads

Overall workflow:
A: ETL pipe: Retrieve most recent PBF files from geofabrik (continent).
B: ETL pipe: Use Osmium to merge PBF files into global composite: osmium merge file1.osm file2.osm -o merged.osm
C: ETL pipe: Run pre-processing stages for input into OSM map server
          osrm-extract -p /opt/car.lua /data/asia.osm.pbf
          osrm-partition /data/asia.osrm
          osrm-customize /data/asia.osrm
D: Launch the server with the processed PBF.
E: Run the distance queries for the global grid.

Note that this uses prefect.io for ETL workflow monitoring / management.
The conda environment created must include prefect; it was tested using prefect 2.19.1.

You will need a pod that runs "prefect server start", and the server information for that pod.
In that server, you need to create a work pool for the K8S nodes we'll be using.  The first time, you'll need to 
log into the UI and setup a work pool (kubernetes?).
