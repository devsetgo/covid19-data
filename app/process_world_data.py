from datetime import date, datetime, timedelta

import pandas as pd
from loguru import logger
from open_data import get_world_data, get_world_pop

_DATE_7 = pd.Timestamp('now').floor('D') + pd.offsets.Day(-7)
_DATE_14 = pd.Timestamp('now').floor('D') + pd.offsets.Day(-14)
_DATE_30 = pd.Timestamp('now').floor('D') + pd.offsets.Day(-30)



def calc_per_million(amount, population):
    if amount is None:
        return 0
    
    if population is None or population == 0:
       return 0
       
    return amount * 1000000 / population



def main():
    wp = get_world_pop()    
    wd = get_world_data()

    logger.info(f"Staring processing of {len(wd.index)} records")
    #join world_data and world_popluation adding poplutaion to dataframe
    wp = wp.rename(columns={'Country':'Country/Region'})
    wd = pd.merge(wd, wp, on=['Country/Region'])

    #TODO 
    wd['MyCountries'] = False    
    
    #covernt column to datetime
    wd['Date'] = pd.to_datetime(wd['Date'])
    wd['WeekNum'] = wd.Date.dt.week    
    wd["Month"] = wd.Date.dt.month
    # wd['DayOfYear'] = wd.Date.dt.dayofyear

    #set booleans
    wd["Last7Days"]  = (wd.Date >= _DATE_7) 
    wd["Last14Days"] = (wd.Date >= _DATE_14) 
    wd["Last30Days"] = (wd.Date >= _DATE_30) 

    #calculate case/death per millions
    wd['CasesPM'] = wd.apply(lambda row : calc_per_million(row['Confirmed'], row['Population']),axis=1)
    wd['DeathsPM'] = wd.apply(lambda row: calc_per_million(row['Deaths'], row['Population']),axis=1)
    
    #calculate rate of chage
    g = wd.sort_values('Date').groupby(['Country/Region','Province/State'])    
    wd["GrowthPerDay"] = g.Confirmed.diff()
    wd["PercentChange"] = g.Confirmed.pct_change()
    wd["DeathPerDay"] = g.Deaths.diff()
    wd["DeathPerChange" ] = g.Deaths.pct_change()

    logger.info('writing file calc_world.csv')    
    wd.to_csv("calc_world.csv", index=False)


if __name__ == "__main__":
    main()
