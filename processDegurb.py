import geopandas
import pymysql


mysql_config_db = {
    'host': 'mariadb-service',  # Your MySQL host/service
    'user': 'root',           # Your MySQL user
    'port': 3306,
    'password': '',           # Your MySQL password
    'db': 'globalroads'
}

logging_path = "/kube/home/logs/globalRoads"
#table: roadresults

def kLog(type, message, logPath=logging_path):
    podName = os.getenv('POD_NAME')
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    with open(logPath + str(podName) + ".log", "a") as f:
        f.write(str(type) + ": " + str(timestamp) + " --- " + str(message) + "\n")

with open("./sourceData/nepalDegurbaPoints.geojson", 'r') as f:
    degUrbPts = geopandas.read_file(f)

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

def processPoints(pt):
    with open("./sourceData/urbanCentroids.geojson", "r") as u:
        urbanPoints = geopandas.read_file(u)
    
    results = {}
    results["latitude"] = 10.0
    results["longitude"] = 5.0
    results["name"] = "Test"
    results["total_population"] = 10049
    results["urbanID"] = 10
    results["distance"] = 10039.23
    results["traveltime"] = 1049.0
                                  
    return(results)

conn = connect_with_retry(mysql_config_db)
insert_results(conn, processPoints("test"))
conn.close()
