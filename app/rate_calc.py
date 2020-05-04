from loguru import logger
from datetime import datetime, timedelta, date


@logger.catch
def country_growth_per_day(
    country: str, province: str, current_day, cases: str, data: dict
):

    if cases == "":
        cases = 0

    date_obj = current_day
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    prev_day = new_date - timedelta(days=1)
    prev_day_cases = None

    for d in data:

        if (
            d["Date"] == str(prev_day)
            and d["Country/Region"] == country
            and d["Province/State"] == province
        ):
            prev_day_cases = d["Confirmed"]
            # print(2,str(prev_day), day_2)
            break

    if prev_day_cases == "" or prev_day_cases == None:
        prev_day_cases = 0
    result = int(cases) - int(prev_day_cases)
    return result


@logger.catch
def country_death_per_day(
    country: str, province: str, current_day, deaths: str, data: dict
):

    if deaths == "":
        deaths = 0

    date_obj = current_day
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    prev_day = new_date - timedelta(days=1)
    prev_day_cases = None

    for d in data:

        if (
            d["Date"] == str(prev_day)
            and d["Country/Region"] == country
            and d["Province/State"] == province
        ):
            prev_day_cases = d["Deaths"]
            break

    if prev_day_cases == "" or prev_day_cases == None:
        prev_day_cases = 0

    result = int(deaths) - int(prev_day_cases)
    return result


def get_country_data(country: str, data: dict):

    country_list: list = []
    for d in data:
        if d["Country/Region"] == country:
            country_list.append(d)
    # if country =="Andorra":
    #     print(country_list)
    return country_list
