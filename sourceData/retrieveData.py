import requests
import geopandas as gpd
import shutil
from pyrosm import OSM, get_data
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import traceback
import gc

# Constants
PARENT_CONTINENTS = ['africa', 'antartica', 'asia', 'central-america', 'europe', 'north-america', 'south-america', 'australia-oceania']
EXCEPTIONS = ['south-africa-and-lesotho', 'alps', 'britain-and-ireland', 'dach', 'us', 'guyana', 'guernsey-jersey', 'american-oceania', 
              'us-midwest', 'us-northeast', 'us-pacific', 'us-south', 'us-west', 'guyana', 'united-kingdom', 'us/california', 'us/us-virgin-islands']
TMPBASEPATH = "/kube/home/tmp/globalRoads"
OUTPUTPATH = "/kube/home/git/globalRoads/sourceData/parquet"
LOGBASEPATH = "/kube/home/logs/globalRoads"
PROCESSES = 2

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

def filter_pbf_to_parquet(pbf_id):
    """
    Converts a PBF file to a Parquet file using GeoPandas, with a filtering step
    for specific road types.

    :param pbf_id: ID of the PBF file.
    :param parquet_file: Path where the Parquet file will be saved.
    """
    pLogger(pbf_id, "INFO", "Beginning filtering and conversion to Parquet file.")
    TMPPATH = TMPBASEPATH + "/" + str(pbf_id) + "/" + pbf_id + "-latest.osm.pbf"
    pLogger(pbf_id, "INFO", "TMPPATH: " + str(TMPPATH))
    parquet_file = OUTPUTPATH + "/" + str(pbf_id) + ".parquet"
    pLogger(pbf_id, "INFO", "Parquet Path: " + str(parquet_file))
    # Define the subset of road types to include
    roads_subset = ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street','track']
    # Initialize the OSM object and read the PBF file
    osm = OSM(TMPPATH)
    # Get the data for a specific layer, here 'driving'
    pLogger(pbf_id, "INFO", "Fetching driving network data from PBF.")
    gdf = osm.get_data_by_custom_criteria(custom_filter={'highway': True})
    # Filter the GeoDataFrame based on the roads subset
    pLogger(pbf_id,"INFO","Filtering by road type.")
    roadways = gdf.loc[gdf['highway'].isin(roads_subset)]
    # Convert to GeoDataFrame if it's not already
    if not isinstance(roadways, gpd.GeoDataFrame):
        pLogger(pbf_id, "INFO", "Convert to GDF.")
        roadways = gpd.GeoDataFrame(roadways)
    # Write the filtered data to Parquet
    pLogger(pbf_id, "INFO", "Writing to Parquet.")
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

def download_feature(feature):
    """
    Processes a single feature from the JSON data.
    """
    properties = feature['properties']
    urls = properties['urls']
    identifier = properties['id']
    filename = identifier
    TMPPATH = TMPBASEPATH + "/" + str(identifier)
    check_and_recreate_folder(TMPPATH)
    
    
    if identifier in ['antarctica', 'norcal', 'socal'] or ('parent' in properties and properties['parent'] in PARENT_CONTINENTS and identifier not in EXCEPTIONS):
        filename = identifier
        pLogger(filename, "INFO", "Beginning download for " + str(filename))
        if identifier == 'antarctica':#or identifier == 'australia-oceania':
            osm = OSM(get_data(identifier, directory = TMPPATH))
        elif identifier == 'norcal': 
            osm = OSM(get_data('northern_california', directory = TMPPATH))
            filename = "northern_california"
        elif identifier == 'socal':
            osm = OSM(get_data('southern_california', directory = TMPPATH))
            filename = "southern_california"
        elif 'parent' in properties:
            pLogger(filename, "INFO", "Parent exception identified, continuing to process " + str(filename))
            if properties['parent'] in PARENT_CONTINENTS and identifier not in EXCEPTIONS:
                parent = properties['parent']
                pLogger(filename, "INFO", 'PARENT 1: ' + str(parent))
                pLogger(filename, "INFO", 'ID: ' + str(identifier))
                pbf_link = urls['pbf']
                if properties['parent'] == 'north-america' and 'iso3166-2' in properties and identifier != 'us/california':
                    updated_id = identifier.replace('us/','')
                    filename = updated_id
                    osm = OSM(get_data(updated_id, directory = TMPPATH))
                elif '/' in identifier:
                    updated_id = identifier.replace('/', '-')
                    filename = updated_id
                    osm = OSM(get_data(updated_id, directory = TMPPATH)) 
                else:
                    fp = get_data(identifier, directory = TMPPATH)
                    osm = OSM(fp)
            else:
                fp = get_data(identifier, directory = TMPPATH)
                osm = OSM(fp)
    
        return(filename)
    else:
        return("PASS")

def process_feature(feature):
    # Combined function to download and convert data
    try:
        featureID = download_feature(feature)
        if(featureID != "PASS"):
            filter_pbf_to_parquet(featureID)
            return([featureID,"DONE"])
        else:
            return([featureID,"PASS"])
    except Exception as e:
        pLogger("MASTER_ERROR", "CRIT", "The feature was unable to be processed: " + str(feature["properties"]))
        pLogger("MASTER_ERROR", "CRIT", "E: " + str(e))
        pLogger("MASTER_ERROR", "CRIT", "Trace: " + str(traceback.format_exc()))
        return([featureID,"ERROR"])

def main():
    """
    Main function to orchestrate the fetching and processing of data.
    """
    data_url = "https://download.geofabrik.de/index-v1.json"
    data = fetch_data(data_url)
    features = data['features']
    pLogger("MASTER", "INFO", "Features: " + str(features))

    with ProcessPoolExecutor(max_workers=PROCESSES) as executor:
        futures = [executor.submit(process_feature, feature) for feature in features]
        for future in as_completed(futures):
            result = future.result()
            pLogger("MASTER", "INFO", "Result Completed: " + str(result))
    
    print(result)

if __name__ == "__main__":
    main()

