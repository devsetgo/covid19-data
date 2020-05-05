import re
import pandas as pd

def get_county_pop() -> pd.DataFrame:
    return pd.read_csv("us_county_population.csv")
    
def get_county_data() -> pd.DataFrame:
    return pd.read_csv("us_counties_data.csv")
    
def get_state_pop()  -> pd.DataFrame:
    return pd.read_csv("us_state_population.csv")
    
def get_state_data()  -> pd.DataFrame:
    return pd.read_csv("us_states_data.csv")
  
def get_world_pop()  -> pd.DataFrame:
    return pd.read_csv("world_population.csv")

def get_world_data()  -> pd.DataFrame:
    return pd.read_csv("world_data.csv")
    
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
