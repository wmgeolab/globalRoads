from prefect import flow, serve, task
import time
import os
from datetime import datetime, timedelta
import requests
import subprocess
import sys

@task(name="Continent Download",
      description="Function to download individual country files from geoFabrik",
      task_run_name="download-{continent}",
      retries=2, retry_delay_seconds=5)
def downloadContinent(continent: str = "none",
                      DOWNLOADPATH: str = "none",
                      STALE_DAYS: int = 7):
    """
    A function which downloads the most recent continent-scale PBFs
    from geofabrik and saves them into a local directory.

    Parameters:
    continent (string): The continent (topmost level) to retrieve data for.
    DOWNLOADPATH (string): The folder to output the retrieved PBF.

    Returns:
    str: Either the path of the download, or raised error code.
    """
    FILEPATH = DOWNLOADPATH + continent + "-latest.osm.pbf"

    if os.path.exists(FILEPATH):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(FILEPATH))
        if datetime.now() - file_mod_time < timedelta(days=STALE_DAYS):
            print(str(FILEPATH) + ": Current file is less than "+str(STALE_DAYS)+" day(s) old. Skipping download.")
            return(FILEPATH)

    url = "https://download.geofabrik.de/"+str(continent)+"-latest.osm.pbf"
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Open a local file in binary write mode
        with open(FILEPATH, 'wb') as file:
            # Write the content of the response to the file
            file.write(response.content)
        print(str(FILEPATH) + ": Successfully downloaded and updated.")
        return(FILEPATH)
    else:
        raise Exception(str(FILEPATH) + ": Error during download.  Status code: " + str(response.status_code))
        





@flow(name="Global Download Flow",
      description="Download all continent files for global road layer.",
      flow_run_name="{TIMESTAMP}",
      log_prints=True)
def downloadGlobe(CONTINENTS: list,
                  DOWNLOADPATH: str,
                  STALE_DAYS: int = 7,
                  TIMESTAMP: str = str(datetime.now())):
    jobs = []
    for continent in CONTINENTS:
        job = downloadContinent.submit(continent, DOWNLOADPATH)
        jobs.append(job)
    
    #Execute the jobs and collect results
    results = []
    for j in jobs:
        results.append(j.result())

    print(results)


@task(name="Build Global PBF",
      log_prints=True)
def buildGlobalPBF(CONTINENTS: list,
                   DOWNLOADPATH: str,
                   GLOBALPBFPATH: str,
                   STALE_DAYS: int = 7):

    if os.path.exists(GLOBALPBFPATH):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(GLOBALPBFPATH))
        if datetime.now() - file_mod_time < timedelta(days=STALE_DAYS):
            print(str(GLOBALPBFPATH) + ": Current file is less than "+str(STALE_DAYS)+" day(s) old. Skipping download.")
            return(GLOBALPBFPATH)
    
    
    #Note we're using the system executable here,
    #which assumes the conda environment main.py is running on
    #is the same one osmium is installed into.
    conda_base = os.path.dirname(os.path.dirname(sys.executable))
    env_bin_path = os.path.join(conda_base, 'envs', "pT", 'bin')
    env = os.environ.copy()
    env['PATH'] = f"{env_bin_path}:{env['PATH']}"
    env['CONDA_PREFIX'] = os.path.join(conda_base, 'envs', "pT")
    
    #Build the command
    command = []
    command.append("osmium")
    command.append("merge")
    for continent in CONTINENTS:
        command.append(DOWNLOADPATH + continent + "-latest.osm.pbf")
    command.append("-o")
    command.append(GLOBALPBFPATH)

    #Now we execute.  
    try:
        result = subprocess.run(command, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Output:\n", result.stdout)
        return(GLOBALPBFPATH)
    except subprocess.CalledProcessError as e:
        print("An error occurred:", e)
        raise Exception("Error output:\n", e.stderr)

    print(DOWNLOADPATH)

@flow(name="globalRoads main",
      description="Full download and build for globalRoads products.",
      flow_run_name="{TIMESTAMP}",
      log_prints=True)
def globalRoads(TIMESTAMP: str = str(datetime.now())):
    CONTINENTS = ["antarctica","australia-oceania", "africa", "asia", "central-america", "north-america", "europe", "south-america"]
    
    TMPBASEPATH = "/sciclone/geograd/globalRoads/tmp"
    DOWNLOADPATH = "/sciclone/geograd/globalRoads/sourceData/"
    GLOBALPBFPATH = "/sciclone/geograd/globalRoads/globalPBF/global-latest-osm.pbf"
    STALE_DAYS = 7
    downloads = downloadGlobe(CONTINENTS,DOWNLOADPATH,STALE_DAYS)
    buildGlobalPBF.submit(CONTINENTS, DOWNLOADPATH, GLOBALPBFPATH, STALE_DAYS, wait_for=[downloads])

if __name__ == "__main__":
    globalRoads()
