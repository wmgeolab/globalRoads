#First, you must create a conda environment with prefect:
#conda create -n pT
#conda activate pT
#conda install -c conda-forge prefect

#After the install, you must then configure prefect from CLI.
#In a local install, the API is set to http://127.0.0.1:4200/api
#We must change to point at our API server:
#https://docs.prefect.io/2.10.12/host/
#prefect config set PREFECT_API_URL="http://128.239.58.222:4200/api"

#Once the above is run, you should be able to type
#"python testFlow.py", and the below should appear on the user interface.

from prefect import flow, serve, task, deploy
from datetime import datetime

@flow(name="Test Flow",
      description="A second test flow for Matt",
      log_prints=True)
def testFlow():
    TIMESTAMP = str(datetime.now())
    print("This is a second test flow, executed at time " + TIMESTAMP)
    return(TIMESTAMP)

testFlow.from_source(
    source="https://github.com/wmgeolab/globalRoads.git",
    entrypoint="testDeploy.py:testFlow"
).deploy(name="testDeploy", work_pool_name="process-agent-GPU")