from devsetgo_lib.file_functions import open_csv, save_csv
from open_data import (
    get_county_pop,
    get_state_pop,
    get_world_pop,
    get_county_data,
    get_state_data,
    get_world_data,
)
from datetime import datetime, timedelta, date
from tqdm import tqdm
from unsync import unsync
from loguru import logger
from rate_calc import country_growth_per_day, country_death_per_day, get_country_data


def calc_seven_day(row_data):

    date_obj = row_data["Date"]
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    # days_seven =  new_date - timedelta(days=7)
    current_obj = datetime.today().date()
    current_days_seven = current_obj - timedelta(days=7)
    if current_days_seven <= new_date <= current_obj:
        result = True
    else:
        result = False
    return result


def calc_fourteen_day(row_date):

    date_obj = row_date["Date"]
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    # days_seven =  new_date - timedelta(days=7)
    current_obj = datetime.today().date()
    current_days_seven = current_obj - timedelta(days=14)
    if current_days_seven <= new_date <= current_obj:
        result = True
    else:
        result = False
    return result


def calc_thrity_day(row_date):

    date_obj = row_date["Date"]
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    # days_seven =  new_date - timedelta(days=7)
    current_obj = datetime.today().date()
    current_days_seven = current_obj - timedelta(days=30)
    if current_days_seven <= new_date <= current_obj:
        result = True
    else:
        result = False
    return result


def get_week_number(row_data):

    date_obj = row_data["Date"]
    new_date = datetime.strptime(date_obj, "%Y-%m-%d")
    result = datetime.date(new_date).isocalendar()[1]
    return result


def get_week_month(row_data):

    date_obj = row_data["Date"]
    dt = datetime.strptime(date_obj, "%Y-%m-%d")
    result = dt.month
    return result


def calc_per_million(country: str, amount: int, population: int):

    if population == 0:
        per_million = 0
    else:
        population = look_up_country(country=country)
        per_million = amount * 1000000 / population

    return per_million


def look_up_country(country: str, data: dict):

    for c in data:
        # print(c)
        if c["Country"] == country:
            result = c["Population"]
    else:
        result = 0
    # population of country
    # print(result)
    return result


@unsync
def run_calculations(wp, w, wd):

    population = look_up_country(country=w["Country/Region"], data=wp)
    country_list: list = get_country_data(country=w["Country/Region"], data=wd)
    week_num = get_week_number(w)

    month = get_week_month(w)

    day_7 = calc_seven_day(w)

    day_14 = calc_fourteen_day(w)

    day_30 = calc_thrity_day(w)

    cpm = calc_per_million(
        country=w["Country/Region"], amount=w["Confirmed"], population=population
    )

    dpm = calc_per_million(
        country=w["Country/Region"], amount=w["Deaths"], population=population
    )

    my_countries = False  # ToDo

    growth_per_day = country_growth_per_day(
        country=w["Country/Region"],
        province=w["Province/State"],
        current_day=w["Date"],
        cases=w["Confirmed"],
        data=country_list,
    )

    case_percent_change = 0.0

    death_per_day = country_death_per_day(
        country=w["Country/Region"],
        province=w["Province/State"],
        current_day=w["Date"],
        deaths=w["Deaths"],
        data=country_list,
    )

    death_percent_change = 0.0

    new_row: list = [
        w["Date"],
        w["Country/Region"],
        w["Province/State"],
        w["Lat"],
        w["Long"],
        w["Confirmed"],
        w["Recovered"],
        w["Deaths"],
        week_num,
        month,
        day_7,
        day_14,
        day_30,
        cpm,
        dpm,
        my_countries,
        growth_per_day,
        case_percent_change,
        death_per_day,
        death_percent_change,
    ]

    return new_row


def main():

    world_data_calculated: list = []
    header: list = [
        "Date",
        "Country/Region",
        "Province/State",
        "Lat",
        "Long",
        "Confirmed",
        "Recovered",
        "Deaths",
        "WeekNum",
        "Month",
        "Last7Days",
        "Last14Days",
        "Last30Days",
        "CasesPM",
        "DeathsPM",
        "MyCountries",
        "GrowthPerDay",
        "PercentChange",
        "DeathPerDay",
        "DeathPerChange",
    ]
    world_data_calculated.append(header)
    count = 0
    wp = get_world_pop()

    wd = get_world_data()

    tasks = [
        run_calculations(wp=wp, w=w, wd=wd)
        for w in tqdm(wd, ascii=True, desc="World_Data_Start")
    ]

    data = [
        task.result() for task in tqdm(tasks, ascii=True, desc="World_Data_Process")
    ]

    for d in data:
        world_data_calculated.append(d)
    # print(world_data_calculated)
    save_csv(file_name="calc_world.csv", data=world_data_calculated)


if __name__ == "__main__":
    main()
