import requests
import geopandas as gpd
import shutil
from pyrosm import OSM, get_data
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import traceback
import gc
import requests

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

TMPBASEPATH = "/kube/home/tmp/globalRoads"
OUTPUTPATH = "/kube/home/git/globalRoads/sourceData/parquet"
LOGBASEPATH = "/kube/home/logs/globalRoads"
PROCESSES = 1

def pLogger(id, type, message, path=LOGBASEPATH):
    with open(path + "/" + str(id) + ".log", "a") as f:
        f.write(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ": (" + str(type) + ") " + str(message) + "\n")

def check_and_recreate_folder(folder_path):
    """
    Check if a folder exists, and if it does, delete and recreate it.
    If it doesn't exist, create it.

    :param folder_path: Path to the folder.
    """
    # Check if the folder exists
    if os.path.exists(folder_path):
        # If it exists, remove the folder and its contents
        shutil.rmtree(folder_path)
    
    # Create the folder
    os.makedirs(folder_path)

def filter_pbf_to_parquet(jobID):
    """
    Converts a PBF file to a Parquet file using GeoPandas, with a filtering step
    for specific road types.
    """

    pLogger(jobID, "INFO", "Beginning filtering and conversion to Parquet file.")
    
    TMPPATH = TMPBASEPATH + "/" + str(jobID)
    FILEPATH = TMPPATH + "/" + str(jobID) + ".osm.pbf"

    pLogger(jobID, "INFO", "FILEPATH: " + str(FILEPATH))
    parquet_file = OUTPUTPATH + "/" + str(jobID) + ".parquet"
    pLogger(jobID, "INFO", "Parquet Path: " + str(parquet_file))

    # Define the subset of road types to include
    roads_subset = ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street','track']
    
    # Initialize the OSM object and read the PBF file
    osm = OSM(FILEPATH)

    # Get the data for a specific layer, here 'driving'
    pLogger(jobID, "INFO", "Fetching driving network data from PBF.")
    gdf = osm.get_data_by_custom_criteria(custom_filter={'highway': True})
    # Filter the GeoDataFrame based on the roads subset
    pLogger(jobID,"INFO","Filtering by road type.")
    roadways = gdf.loc[gdf['highway'].isin(roads_subset)]
    # Convert to GeoDataFrame if it's not already
    if not isinstance(roadways, gpd.GeoDataFrame):
        pLogger(jobID, "INFO", "Convert to GDF.")
        roadways = gpd.GeoDataFrame(roadways)
    # Write the filtered data to Parquet
    pLogger(jobID, "INFO", "Writing to Parquet.")
    roadways['id'] = roadways['id'].astype(float).astype(int)
    roadways.to_parquet(parquet_file)
    del roadways, gdf, osm
    gc.collect()

def fetch_data(url):
    """
    Fetches and returns JSON data from the specified URL.
    """
    response = requests.get(url)
    return response.json()

def download_feature(url, jobID):
    """
    Downloads a layer from OSM.
    """
    TMPPATH = TMPBASEPATH + "/" + str(jobID)
    FILEPATH = TMPPATH + "/" + str(jobID) + ".osm.pbf"
    check_and_recreate_folder(TMPPATH)
    
    try:
        pLogger(jobID, "INFO", "Downloading: " + str(url))
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            # Open a local file in binary write mode
            with open(FILEPATH, 'wb') as file:
                # Write the content of the response to the file
                file.write(response.content)
            pLogger(jobID, "INFO", "File downloaded.")
            return("PASS")
        else:
            pLogger(jobID, "CRIT", "Failed to retrieve the file. Status code: " + str(response.status_code))
            return("FAIL")
    except Exception as e:
        pLogger(jobID, "CRIT", "Failed to retrieve the file with an exception. Error: " + str(e))
        return("FAIL")

def process_file(url, jobID):
    # Combined function to download and convert data
    try:
        parquet_file = OUTPUTPATH + "/" + str(jobID) + ".parquet"
        if not os.path.exists(parquet_file):
            downloadOutcome = download_feature(url, jobID)
            
            if(downloadOutcome != "FAIL"):
                filter_pbf_to_parquet(jobID)
                pLogger("MASTER", "INFO", str(jobID) + " DONE.")
                return([jobID,"DONE"])
            else:
                pLogger("MASTER_ERROR", "CRIT", "Download failed for: " + str(jobID) + " with URL: " + str(url))
                return([jobID,"FAIL"])
        else:
            pLogger("MASTER", "INFO", str(jobID) + " parquet file already exists. Skipping.")
            return([jobID, "ALREADY EXISTS"])
    except Exception as e:
        pLogger("MASTER_ERROR", "CRIT", "The feature was unable to be processed: " + str(jobID))
        pLogger("MASTER_ERROR", "CRIT", "E: " + str(e))
        pLogger("MASTER_ERROR", "CRIT", "Trace: " + str(traceback.format_exc()))
        return([jobID,"ERROR"])

def main():
    """
    Main function to orchestrate the fetching and processing of data.
    """
    for url in LINKS:
        jobID = url.split("/")[3].split(".")[0].split("-")[0]
        pLogger("MASTER", "INFO", "Processing: " + str(url))
        output = process_file(url, jobID)
        pLogger("MASTER", "INFO", "Result Completed: " + str(output))
        del output
        gc.collect()

    # with ProcessPoolExecutor(max_workers=PROCESSES) as executor:
    #     futures = [executor.submit(process_feature, feature) for feature in features]
    #     for future in as_completed(futures):
    #         result = future.result()
    #         pLogger("MASTER", "INFO", "Result Completed: " + str(result))


if __name__ == "__main__":
    main()

