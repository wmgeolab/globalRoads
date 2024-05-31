import requests
import folium
import polyline
import time 

def exponential_backoff_request(url, retries, base_wait=1):
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

# Define your URL and number of retries here
url = "http://osrm:80/route/v1/driving/27.718107,85.317312;27.706190,85.316324"
retries = 5  # Set the number of retries
base_wait = 1  # Set the base wait time in seconds

result = exponential_backoff_request(url, retries, base_wait)

if result:
    print("Request succeeded!")
    print(result)
else:
    print("Request failed after all retries.")