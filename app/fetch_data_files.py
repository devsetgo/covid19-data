import io

import pandas as pd
import requests
from pathlib import Path
from loguru import logger


def download(url, filename):

    with requests.Session() as s:
        download = s.get(url)

        decoded_content = download.content.decode("utf-8")
        df = pd.read_csv(io.StringIO(decoded_content))
        df.to_csv(str(filename), index=False)

def download_world_population(output_filename):        
        url = "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_TotalPopulationBySex.csv"
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode("utf-8")
            df = pd.read_csv(io.StringIO(decoded_content))
            #filter by year 2020 and using medium fertility variant
            df = df[ (df.Time==2020) & (df.VarID==2)]
            df = df[['Location','PopTotal']]
            df = df.rename(columns={'Location':'Country','PopTotal':'Population'})
            df.to_csv(str(output_filename),index=False)

            

def start_downloads():

    if not Path("world_population.csv").exists():
        download_world_population("world_population.csv")
    
    us_states = {
        "filename": "us_states_data.csv",
        "url": "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv",
    }
    us_counties = {
        "filename": "us_counties_data.csv",
        "url": "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv",
    }
    world = {
        "filename": "world_data.csv",
        "url": "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv",
    }
    ulrs = [us_counties, us_states, world]
    for d in ulrs:
        logger.info(f"downloading {d}")
        download(filename=d["filename"], url=d["url"])


if __name__ == "__main__":

    start_downloads()
