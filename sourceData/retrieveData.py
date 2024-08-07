import requests
import geopandas as gpd
import shutil
from datetime import datetime, timedelta
import os
import traceback
import gc
from prefect import flow, task
from prefect_dask import DaskTaskRunner

# Constants

LINKS=["https://download.geofabrik.de/antarctica-latest.osm.pbf",
       "https://download.geofabrik.de/africa-latest.osm.pbf",
       "https://download.geofabrik.de/asia-latest.osm.pbf",
       "https://download.geofabrik.de/australia-oceania-latest.osm.pbf",
       "https://download.geofabrik.de/central-america-latest.osm.pbf",
       "https://download.geofabrik.de/europe-latest.osm.pbf",
       "https://download.geofabrik.de/north-america-latest.osm.pbf",
       "https://download.geofabrik.de/south-america-latest.osm.pbf"
      ]

TMPBASEPATH = "/tmp"
OUTPUTPATH = "/sciclone/geograd/_deployed/globalRoads/sourceData/parquet"
ORIGINALPATH = "/sciclone/geograd/_deployed/globalRoads/sourceData/original"
LOGBASEPATH = "/sciclone/geograd/_deployed/globalRoads/logs"
STALE_DAYS = 3

def pLogger(id, type, message, path=LOGBASEPATH):
    with open(path + "/" + str(id) + ".log", "a") as f:
        f.write(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ": (" + str(type) + ") " + str(message) + "\n")

def check_and_recreate_folder(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    os.makedirs(folder_path)

def convertToGeoJSON(jobID):
    TMPPATH = TMPBASEPATH + "/" + str(jobID)
    FILEPATH = TMPPATH + "/" + str(jobID) + ".geojson"
    pbfInput = TMPPATH + "/" + str(jobID) + ".osm.pbf"

    if os.path.isfile(FILEPATH):
        os.remove(FILEPATH)
    command = 'ogr2ogr -f GeoJSON ' + FILEPATH + " " + pbfInput + " lines"
    pLogger(jobID, "INFO", "Preparing to convert to geoJSON: " + str(command))
    os.system(command)
    pLogger(jobID, "INFO", "geoJSON Conversion Successful")

@task
def filtergeoJson_createParquet(jobID):
    try:
        parquet_file = OUTPUTPATH + "/" + str(jobID) + ".parquet"
        pLogger(jobID, "INFO", "Beginning geoJSON-based Filtering")
        TMPPATH = TMPBASEPATH + "/" + str(jobID)
        geoJSONPath = TMPPATH + "/" + str(jobID) + ".geojson"

        if os.path.exists(parquet_file):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(parquet_file))
            if datetime.now() - file_mod_time < timedelta(days=STALE_DAYS):
                pLogger(jobID, "INFO", "Parquet file is up-to-date. Skipping filtering and creation.")
                return "SKIP"

        jsonOSM = gpd.read_file(geoJSONPath)
        pLogger(jobID, "INFO", "geoJSON Loaded, moving into filtering.")
        roadsSubset = ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'motorway_link', 'trunk_link',
                    'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'track']

        roadways = jsonOSM.loc[jsonOSM['highway'].isin(roadsSubset)]
        pLogger(jobID, "INFO", "geoJSON filtered, saving as Parquet.")
        roadways.to_parquet(parquet_file)
    except Exception as e:
        pLogger(jobID, "ERROR", str(e))

@task
def fetch_data(url):
    response = requests.get(url)
    return response.json()

@task
def download_feature(url, jobID):
    TMPPATH = TMPBASEPATH + "/" + str(jobID)
    FILEPATH = ORIGINALPATH + "/" + str(jobID) + ".osm.pbf"

    if os.path.exists(FILEPATH):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(FILEPATH))
        if datetime.now() - file_mod_time < timedelta(days=STALE_DAYS):
            pLogger(jobID, "INFO", "File is up-to-date. Skipping download.")
            return "SKIP"

    check_and_recreate_folder(TMPPATH)
    
    try:
        pLogger(jobID, "INFO", "Downloading: " + str(url))
        response = requests.get(url)
        if response.status_code == 200:
            with open(FILEPATH, 'wb') as file:
                file.write(response.content)
            pLogger(jobID, "INFO", "File downloaded.")
            return "PASS"
        else:
            pLogger(jobID, "CRIT", "Failed to retrieve the file. Status code: " + str(response.status_code))
            return "FAIL"
    except Exception as e:
        pLogger(jobID, "CRIT", "Failed to retrieve the file with an exception. Error: " + str(e))
        return "FAIL"

@task
def process_file(url, jobID):
    try:
        parquet_file = OUTPUTPATH + "/" + str(jobID) + ".parquet"
        if not os.path.exists(parquet_file):
            downloadOutcome = download_feature(url, jobID)
            
            if(downloadOutcome != "FAIL"):
                convertToGeoJSON(jobID)
                pLogger("MASTER", "INFO", str(jobID) + " master loop moving into filtering.")
                filtergeoJson_createParquet(jobID)
                pLogger("MASTER", "INFO", str(jobID) + " DONE.")
                return [jobID,"DONE"]
            else:
                pLogger("MASTER_ERROR", "CRIT", "Download failed for: " + str(jobID) + " with URL: " + str(url))
                return [jobID,"FAIL"]
        else:
            pLogger("MASTER", "INFO", str(jobID) + " parquet file already exists. Skipping.")
            return [jobID, "ALREADY EXISTS"]
    except Exception as e:
        pLogger("MASTER_ERROR", "CRIT", "The feature was unable to be processed: " + str(jobID))
        pLogger("MASTER_ERROR", "CRIT", "E: " + str(e))
        pLogger("MASTER_ERROR", "CRIT", "Trace: " + str(traceback.format_exc()))
        return [jobID,"ERROR"]

@task
def process_links(urls):
    results = []
    for url in urls:
        jobID = url.split("/")[3].split(".")[0].split("-")[0]
        pLogger("MASTER", "INFO", "Processing: " + str(url))
        result = process_file(url, jobID)
        pLogger("MASTER", "INFO", "Result Completed: " + str(result))
        results.append(result)
        del result
        gc.collect()
    return results

@flow(task_runner=DaskTaskRunner())
def osm_data_processing(urls):
    process_links(urls)

if __name__ == "__main__":
    osm_data_processing(LINKS)
