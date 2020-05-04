from devsetgo_lib.file_functions import open_csv, save_csv
import re


def get_county_pop() -> dict:

    result = open_csv(file_name="us_county_population.csv")
    return result


def get_county_data() -> dict:

    result = open_csv(file_name="us_counties_data.csv")
    return result


def get_state_pop() -> dict:

    result = open_csv(file_name="us_state_population.csv")
    return result


def get_state_data() -> dict:

    result = open_csv(file_name="us_states_data.csv")
    return result


def get_world_pop() -> dict:

    result = open_csv(file_name="world_population.csv")
    return result


def get_world_data() -> dict:

    result = open_csv(file_name="world_data.csv")
    return result


if __name__ == "__main__":
    result = get_county_pop()
    print(len(result))
    result = get_state_pop()
    print(len(result))
    result = get_world_pop()
    print(len(result))
    result = get_county_data()
    print(len(result))
    result = get_state_data()
    print(len(result))
    result = get_world_data()
    print(len(result))
