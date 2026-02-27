import requests
from zipfile import ZipFile
import os
from pathlib import Path
import asyncio
import aiohttp
import time



download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]



    
def is_url_reachable(url, timeout=5):
    try:
        try:
            response = requests.head(url,allow_redirects=True, timeout=timeout)
            return response.status_code < 400
        except:
            response = requests.get(url,stream=True,timeout=timeout)
            return response.status_code < 400
    except requests.RequestException:
        return False



def download(url, local_path, timeout = 30, chunk_size = 8192):
    '''
    Download file with proper error handling and timeout
    '''
    if is_url_reachable(url):
        try:
            # Validate URL format
            if not url.startswith(('http://','https://')):
                raise ValueError("Invalid URL format")
            
            # Create directory if it doesn't exist
            Path(local_path).parent.mkdir(parents = True, exist_ok=True)

            # Make request with timeout
            response = requests.get(url,timeout = timeout, stream = True)
            response.raise_for_status()


            # Check available disk space
            file_size = int(response.headers.get('content-length', 0))
            if file_size > 0:
                free_space = os.statvfs(Path(local_path).parent).f_bavail * os.statvfs(Path(local_path).parent).f_frsize
                if file_size > free_space:
                    raise OSError("Insufficient disk space")
                
        
        
            # Download and write file
                with open(local_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk: 
                            file.write(chunk)
                        

            return local_path
        
        except requests.exceptions.Timeout:
            raise TimeoutError(f'Download timed out after {timeout} seconds')
        except requests.exceptions.HTTPError as e:
            raise Exception(f'HTTP error {response.status_code} : {e}')
        except requests.RequestException as e:
            raise Exception(f"Request failde: {e}")
        except OSError as e:
            raise Exception(f"File system error: {e}")
        
    else:
        print(f"Url {url} is not reachable")







def main():
    
    
    # Downloading files then unzipping them
    for url in download_uris:
        file_name = url.split('/')[-1]
        local_path = os.path.join(os.getcwd(),'download',file_name)

        with ZipFile(download(url,local_path),'r') as zip_file:
            files = zip_file.namelist()
            members = []

            # Unzipping only csv files of parent folder
            for file in files:
                if file.endswith('.csv') and not '/' in file:
                    members.append(file)
           
            zip_file.extractall(path = Path(local_path).parent, members=members)
            # Removing zip file 
            os.remove(local_path)
    

if __name__ == "__main__":
    start = time.perf_counter()
    main()
    finish = time.perf_counter()

    print(f"{finish - start} seconds passed")
