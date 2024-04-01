import geopandas
import pymysql
import os
from datetime import datetime
import sys
import requests
import time
import warnings
import traceback 

mysql_config_db = {
    'host': 'mariadb-service',  # Your MySQL host/service
    'user': 'root',           # Your MySQL user
    'port': 3306,
    'password': '',           # Your MySQL password
    'db': 'globalroads'
}

logging_path = "/kube/home/logs/globalRoads"
#table: roadresults

RETRIES = 5
RESPONSEWAIT = 5

def kLog(type, message, logPath=logging_path):
    podName = os.getenv('POD_NAME')
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    with open(logPath + str(podName) + ".log", "a") as f:
        f.write(str(type) + ": " + str(timestamp) + " --- " + str(message) + "\n")

def connect_with_retry(config, max_attempts=120, delay_seconds=5):
    """Attempt to connect to MySQL with retries."""
    attempt = 0
    while attempt < max_attempts:
        try:
            conn = pymysql.connect(**config)
            kLog("INFO", "Successfully connected to MySQL.")
            return conn
        except pymysql.Error as e:
            attempt += 1
            kLog("WARN", f"Attempt {attempt} failed: {e}")
            time.sleep(delay_seconds)
    kLog("CRIT", "Exceeded maximum connection attempts.")
    raise Exception("Exceeded maximum connection attempts")

def insert_results(conn, results):
    # SQL statement for inserting data
    query = """INSERT INTO roadresults (latitude, longitude, name, total_population, urbanID, distance, traveltime, dest_latitude, dest_longitude, dest_ID) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    try:
        with conn.cursor() as cursor:
            # Execute the SQL command with values from the results dictionary
            cursor.execute(query, (results["latitude"], 
                                   results["longitude"], 
                                   results["name"], 
                                   results["total_population"], 
                                   results["urbanID"],
                                   results["distance"],
                                   results["traveltime"],
                                   results["dest_latitude"],
                                   results["dest_longitude"],
                                   results["dest_ID"]))
        # Commit the changes to the database
        conn.commit()
    except pymysql.Error as e:
        print(f"Error: {e}")
        conn.rollback()  # Rollback in case of error

def osm_request(url, retries, base_wait=1):
    for retry in range(retries):
        try:
            r = requests.get(url, timeout=10, allow_redirects=False)
            r.raise_for_status()  # Raise an exception for HTTP errors
            res = r.json()
            return res  # Success, return the result
        except requests.exceptions.RequestException as e:
            print(f"Attempt {retry + 1}/{retries}: Request failed - {str(e)}")
            
            if retry < retries - 1:
                # Calculate the wait time exponentially increasing
                wait_time = base_wait * (2 ** retry)
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    
    return None  # All retries failed

def processPoints(pts, conn):
    print("Processing " + str(len(pts)) + " total locations.")
    total = 0
    with open("./sourceData/urbanCentroids.geojson", "r") as u:
        urbanPoints = geopandas.read_file(u)
    urbanPoints.crs = {'proj': 'moll', 'lon_0': 0, 'datum': 'WGS84'}
    urbanPoints = urbanPoints.to_crs(epsg=4326)
    
    #List to hold all results
    distanceResults = []

    
    for index, row in pts.iterrows():
        print("Starting job " + str(total+1) + " of " + str(len(pts)))
        mindist = 9999999999.0
        total = total + 1
        results = {}

        #Identify 5 closest urban areas as the crow flies.
        #We'll then calculate driving duration for all of them,
        #and select the closest as our match.
        
        #Reset from any past runs
        urbanPoints["distance"] = 999999999999
        #Ignoring the projection errors for distance calculations - we're just filtering here, so 
        #don't need perfect accuracy.  The actual distances we use will be calculated by the OSM routing
        #server in the next step.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            urbanPoints["distance"] = urbanPoints.geometry.distance(row.geometry)

        closestPts = urbanPoints.nsmallest(5, 'distance')
        
        from_lon = row.geometry.y
        from_lat = row.geometry.x

        for index_urbcent, row_urbcent in closestPts.iterrows():
            to_lon = row_urbcent.geometry.y
            to_lat = row_urbcent.geometry.x

            url = "http://osrm:80/route/v1/driving/" + str(from_lat) + "," + str(from_lon) + ";" + str(to_lat) + "," + str(to_lon) + "?overview=false&steps=true"
            print(url)
            query = osm_request(url, RETRIES, RESPONSEWAIT)

            dist = 0
            dur = 0
            for i in query["routes"][0]["legs"][0]["steps"]:
                #print(str(i["name"]) + ", " + str(i["distance"]) + "," + str(i["maneuver"]["type"]))
                dist = dist + float(i["distance"])
                dur = dur + float(i["duration"])

            duration = dur
            distance = dist
            print(distance)

            if(distance == 0):
                distance = 9999999999.0

            if(float(mindist) > float(distance)):
                mindist = float(distance)
                results["latitude"] = str(from_lat)
                results["longitude"] = str(from_lon)
                results["name"] = str(row_urbcent["CIESIN_NAME_TL"])
                try:
                    results["total_population"] = str(row_urbcent["Total_Pop"])
                except:
                    results["total_population"] = str(0)
                
                results["urbanID"] = str(row["PID"])
                try:
                    results["distance"] = str(distance)
                    results["traveltime"] = str(duration)
                except:
                    results["distance"] = "99999.0"
                    results["traveltime"] = "99999.0"
            
                results["dest_latitude"] = str(to_lat)
                results["dest_longitude"] = str(to_lon)
                results["dest_ID"] = str(row_urbcent["UID"])
        if(results == {}):
            print("Error in calculating route for " + str(from_lat) + ";" + str(from_lon) + ": " + str(query))
            print("Request: " + str(url))
        else:
            distanceResults.append(results)

        #sys.exit()

        #Commit to MySQL every N observations.
        if(len(distanceResults) >= 2):
            for r in distanceResults:
                try:
                    insert_results(conn, r)
                except Exception as e: 
                    print("CRITICAL FAILURE: SQL Insert failed: " + str(e))
                    print(r)
                    traceback.print_exc()
            distanceResults= []
                                  
    return(distanceResults)

with open("./sourceData/nepalDegurbaPoints.geojson", 'r') as f:
    degUrbPts = geopandas.read_file(f)

degUrbPts.crs = {'proj': 'moll', 'lon_0': 0, 'datum': 'WGS84'}
degUrbPts = degUrbPts.to_crs(epsg=4326)
#Subset for dev
#degUrbExampleSubset = degUrbPts.head()

conn = connect_with_retry(mysql_config_db)
print(processPoints(degUrbPts, conn))
conn.close()