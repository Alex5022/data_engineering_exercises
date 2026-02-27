import asyncio
import aiohttp
import aiofiles
from zipfile import ZipFile
from pathlib import Path
import os



DOWNLOAD_DIR = Path(f"downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

semaphore = asyncio.Semaphore(10)




async def download_file(session, url):
    filename = url.split('/')[-1]
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()

                filepath = DOWNLOAD_DIR / filename

                async with aiofiles.open(filepath, "wb") as file:
                    async for chunk in response.content.iter_chunked(4096):
                        await file.write(chunk)
                print(f"Downloaded: {filename}")
        except aiohttp.ClientResponseError as e:
            print(f"File {filename} was not downloaded.{e.message}")




def unzip_file(filepath):
    if str(filepath).endswith('.zip'):
        with ZipFile(filepath,'r') as zip_file:
                files = zip_file.namelist()
                members = []

                # Unzipping only csv files of parent folder
                for file in files:
                    if file.endswith('.csv') and not '/' in file:
                        members.append(file)
            
                zip_file.extractall(path = Path(filepath).parent, members=members)
                print(f"File {filepath} was extracted")
                # Removing zip file 
                os.remove(filepath)
   




async def download_files(urls):

    async with aiohttp.ClientSession() as session:
        tasks = []

        for url in urls:
            tasks.append(download_file(session,url))

        await asyncio.gather(*tasks)
        


async def main():
    urls = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

    
    await download_files(urls)

     

if __name__ == "__main__":

    asyncio.run(main())
    
    for file_name in os.listdir(DOWNLOAD_DIR):
        file_path = DOWNLOAD_DIR / file_name
        unzip_file(file_path)

  

    
   





