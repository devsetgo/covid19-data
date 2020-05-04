import requests
from devsetgo_lib.file_functions import save_csv

import csv
import requests

# CSV_URL = 'http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv'


def dowload(CSV_URL, filename):

    with requests.Session() as s:
        download = s.get(CSV_URL)

        decoded_content = download.content.decode("utf-8")

        cr = csv.reader(decoded_content.splitlines(), delimiter=",")

        my_list = list(cr)
        save_csv(file_name=filename, data=my_list)
        # for row in my_list:
        #     print(row)


def start_downloads():
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
        dowload(filename=d["filename"], CSV_URL=d["url"])


if __name__ == "__main__":

    start_downloads()
