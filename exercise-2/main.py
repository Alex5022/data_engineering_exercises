import requests
import pandas
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

def dict_rows(url):
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')

    l = []
    li = []
    
    table_rows = soup.find_all('tr',)
    for row in table_rows:
        cells = [td.get_text(strip = True)
        for td in row.find_all("td")]
        if len(cells) > 1:
            del cells[-1]
            l.append(tuple(cells))
    
    del l[0]
    for row in l:
        cell_1,cell_2,cell_3 = row
    
        d = {
            'url':cell_1,
            'time': cell_2,
            'size': cell_3
        }
        
        li.append(d)
    
    return li


def find_rows_by_time(rows, time):
    rows = dict_rows(URL)
    for row in rows:
        if row['time'] == time:
            return row
    return 'Not Found'
    


def main():
    rows = dict_rows(URL)

    row_data = find_rows_by_time(rows,'2024-01-19 14:57')
    url_to_download = URL+ row_data['url']

    try:
        response = requests.get(url_to_download)
        response.raise_for_status()

        with open(row_data['url'], mode='wb') as file:
            for chunk in response.iter_content(4096):
                file.write(chunk)
    except requests.HTTPError as e:
        print(e)
    
    weather_data = pd.read_csv('01044099999.csv')
    max_row = weather_data.loc[weather_data['HourlyDryBulbTemperature'].idxmax()]
    print(max_row)
    
    
       

 


if __name__ == "__main__":
    main()
