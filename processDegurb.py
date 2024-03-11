import geopandas
import pymysql
import os
from datetime import datetime
import sys


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
    query = """INSERT INTO roadresults (latitude, longitude, name, total_population, urbanID, distance, traveltime) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    
    try:
        with conn.cursor() as cursor:
            # Execute the SQL command with values from the results dictionary
            cursor.execute(query, (results["latitude"], 
                                   results["longitude"], 
                                   results["name"], 
                                   results["total_population"], 
                                   results["urbanID"],
                                   results["distance"],
                                   results["traveltime"]))
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

def processPoints(pts):
    with open("./sourceData/urbanCentroids.geojson", "r") as u:
        urbanPoints = geopandas.read_file(u)
    
    urbanPoints = urbanPoints.to_crs(epsg=4326)
    mindist = 9999999.0
    for index, row in pts.iterrows():
        print(index)
        print(row.geometry.x)
        print(row.geometry.y)
        print("-----")

        from_lat = row.geometry.y
        from_lon = row.geometry.x

        for index_urbcent, row_urbcent in urbanPoints.iterrows():
            to_lat = row_urbcent.geometry.y
            to_lon = row_urbcent.geometry.x

            distStr = "http://osrm:80/route/v1/driving/" + str(from_lat) + "," + str(from_lon) + ";" + str(to_lat) + "," + str(to_lon)

            result = osm_request(url, RETRIES, RESPONSEWAIT)
            print(result)
            sys.exit()

    results = {}
    results["latitude"] = 10.0
    results["longitude"] = 5.0
    results["name"] = "Test"
    results["total_population"] = 10049
    results["urbanID"] = 10
    results["distance"] = 10039.23
    results["traveltime"] = 1049.0
                                  
    return(results)

with open("./sourceData/nepalDegurbaPoints.geojson", 'r') as f:
    degUrbPts = geopandas.read_file(f)

degUrbPts.crs = {'proj': 'moll', 'lon_0': 0, 'datum': 'WGS84'}
degUrbPts = degUrbPts.to_crs(epsg=4326)
degUrbExampleSubset = degUrbPts.head()

processPoints(degUrbExampleSubset)

#conn = connect_with_retry(mysql_config_db)
#insert_results(conn, processPoints("test"))
#conn.close()
