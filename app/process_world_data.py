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
        if c["name"] == country:
            result = c["pop2020"]
    else:
        result = 0
    # population of country
    # print(result)
    return result


def calc_growth_per_day(country: str, current_day, data: dict):

    date_obj = current_day
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    prev_day = new_date - timedelta(days=1)
    day_1 = None
    day_2 = None
    # print(type(current_day),type(str(prev_day)))
    for d in data:

        if d["Date"] == current_day and d["Country/Region"] == country:
            day_1 = d["Confirmed"]
            # print(1,current_day, day_1,d["Country/Region"],country)
            break

    for d in data:

        if d["Date"] == str(prev_day) and d["Country/Region"] == country:
            day_2 = d["Confirmed"]
            # print(2,str(prev_day), day_2)
            break

    if day_1 is None:
        day_1 = 0

    if day_2 is None:
        day_2 = 0
    # print(country, current_day, day_2, prev_day, day_1)
    result = int(day_1) - int(day_2)
    # print(result)
    return result


def calc_death_per_day(country: str, current_day, data: dict):

    date_obj = current_day
    new_date = datetime.strptime(date_obj, "%Y-%m-%d").date()
    prev_day = new_date - timedelta(days=1)
    day_1 = None
    day_2 = None
    # print(type(current_day),type(str(prev_day)))
    for d in data:

        if d["Date"] == current_day and d["Country/Region"] == country:
            day_1 = d["Deaths"]
            # print(1,current_day, day_1,d["Country/Region"],country)
            break

    for d in data:

        if d["Date"] == str(prev_day) and d["Country/Region"] == country:
            day_2 = d["Deaths"]
            # print(2,str(prev_day), day_2)
            break

    if day_1 is None:
        day_1 = 0

    if day_2 is None:
        day_2 = 0
    # print(country, current_day, day_2, prev_day, day_1)
    result = int(day_1) - int(day_2)
    # print(result)
    return result

@unsync
def run_calculations(wp, w, wd):
    new_row: list = [
        w["Date"],
        w["Country/Region"],
        w["Province/State"],
        w["Lat"],
        w["Long"],
        w["Confirmed"],
        w["Recovered"],
        w["Deaths"],
    ]

    population = look_up_country(country=w["Country/Region"], data=wp)

    week_num = get_week_number(w)
    new_row.append(week_num)

    month = get_week_month(w)
    new_row.append(month)

    day_7 = calc_seven_day(w)
    new_row.append(day_7)

    day_14 = calc_fourteen_day(w)
    new_row.append(day_14)

    day_30 = calc_thrity_day(w)
    new_row.append(day_30)

    cpm = calc_per_million(
        country=w["Country/Region"], amount=w["Confirmed"], population=population
    )
    new_row.append(cpm)

    dpm = calc_per_million(
        country=w["Country/Region"], amount=w["Deaths"], population=population
    )
    new_row.append(dpm)

    my_countries = False  # ToDo
    new_row.append(my_countries)

    growth_per_day = calc_growth_per_day(
        country=w["Country/Region"], current_day=w["Date"], data=wd
    )
    new_row.append(growth_per_day)

    case_percent_change = 0.0
    new_row.append(case_percent_change)

    death_per_day = calc_death_per_day(
        country=w["Country/Region"], current_day=w["Date"], data=wd
    )
    new_row.append(death_per_day)

    death_percent_change = 0.0
    new_row.append(death_percent_change)

    # world_data_calculated.append(new_row)
    return new_row


def main():
    # Date	Country/Region	Province/State	Lat	Long	Confirmed	Recovered	Deaths	WeekNum	Month	Last7Days	Last14Days	Last30Days	CasesPM	DeathsPM	MyCountries	GrowthPerDay	PercentChange	DeathPerDay	DeathPerChange
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

    tasks = [run_calculations(wp=wp, w=w, wd=wd) for w in tqdm(wd,ascii=True,desc="World_Data_Start")]

    data = [task.result() for task in tqdm(tasks,ascii=True,desc="World_Data_Process")]
    
    for d in data:
        world_data_calculated.append(d)
    # print(world_data_calculated)
    save_csv(file_name="calc_world.csv", data=world_data_calculated)
    # result = "done"
    # return result

    # for w in tqdm(wd):
    #     # print(w['Country/Region'])
    #     new_row: list = [
    #         w["Date"],
    #         w["Country/Region"],
    #         w["Province/State"],
    #         w["Lat"],
    #         w["Long"],
    #         w["Confirmed"],
    #         w["Recovered"],
    #         w["Deaths"],
    #     ]
    #     count += 1
    #     population = look_up_country(country=w["Country/Region"], data=wp)

    #     week_num = get_week_number(w)
    #     new_row.append(week_num)

    #     month = get_week_month(w)
    #     new_row.append(month)

    #     day_7 = calc_seven_day(w)
    #     new_row.append(day_7)

    #     day_14 = calc_fourteen_day(w)
    #     new_row.append(day_14)

    #     day_30 = calc_thrity_day(w)
    #     new_row.append(day_30)

    #     cpm = calc_per_million(
    #         country=w["Country/Region"], amount=w["Confirmed"], population=population
    #     )
    #     new_row.append(cpm)

    #     dpm = calc_per_million(
    #         country=w["Country/Region"], amount=w["Deaths"], population=population
    #     )
    #     new_row.append(dpm)

    #     my_countries = False  # ToDo
    #     new_row.append(my_countries)

    #     growth_per_day = calc_growth_per_day(
    #         country=w["Country/Region"], current_day=w["Date"], data=wd
    #     )
    #     new_row.append(growth_per_day)

    #     case_percent_change = 0.0
    #     new_row.append(case_percent_change)

    #     death_per_day = calc_death_per_day(
    #         country=w["Country/Region"], current_day=w["Date"], data=wd
    #     )
    #     new_row.append(death_per_day)

    #     death_percent_change = 0.0
    #     new_row.append(death_percent_change)

    #     world_data_calculated.append(new_row)
    #     # if count == 100:
    #     #     break


if __name__ == "__main__":
    main()
