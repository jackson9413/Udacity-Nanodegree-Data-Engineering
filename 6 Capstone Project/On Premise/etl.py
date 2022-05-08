import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine
from sql_queries import create_and_insert_normalized_table_queries

def et_staging_airports(table, sourceFile):
    """
    ET for staging table airports.
    INPUT: 
        table : table name "airports"
        sourceFile: source file path for "airports"
    OUTPUT:
        return pandas data frame
    """
    # read data as "string" format to ensure all the information is correct
    airport = pd.read_csv(sourceFile, dtype="str")
    # US airport 
    airportUS = (airport.loc[(airport.iso_country == "US") & ~(airport.type == "closed") & airport.type.str.contains("airport")])     
    
    # change "elevation_ft" to numeric
    airportUS["elevation_ft"] = airportUS["elevation_ft"].astype(float)    
    # change "elevation_ft" to numeric
    airportUS["elevation_ft"] = airportUS["elevation_ft"].astype(float)
    # separate "iso_region" to "country" and "region"
    airportUS[["country", "region"]] = airportUS["iso_region"].str.split("-", 1, expand = True)     
    # separate "iso_region" to "country" and "region" and round 4
    airportUS[["longitude", "latitude"]] = airportUS["coordinates"].str.split(",", 1, expand = True).astype(float).round(4)  
    
    # drop unnecessary columns 
    airportUSNew = (airportUS.drop(["iso_country", "iso_region", "coordinates", "country", "continent","iata_code", "gps_code", "local_code", "municipality"], axis=1))
    
    # drop NaN
    airportUSNewNoNa = airportUSNew.dropna()    
    
    return airportUSNewNoNa

    # load to sqlite database
    # insert airportd from pandas dataframe
    #airportUSNewNoNa.to_sql(table, con=con, if_exists="replace", index=False) 
    
def et_staging_cities(table, sourceFile):
    """
    ET for staging table cities.
    INPUT: 
        table : table name "cities"
        sourceFile: source file path for "cities"
    OUTPUT:
        return pandas data frame            
    """
    # read data as "string" format to ensure all the information is correct and assign delimiter ";"
    cities = pd.read_csv(sourceFile, delimiter=";", dtype="str")
            
    # drop NaN
    citiesNoNa = cities.dropna()    
    
    # transform from long to wide
    citiesPop = citiesNoNa[['City', 'State', 'Median Age', 'Male Population',
           'Female Population', 'Total Population', 'Number of Veterans',
           'Foreign-born', 'Average Household Size', 'State Code']]
    citiesNoNa["Count"] = citiesNoNa.Count.astype(float)
    citiesRace = (pd.pivot_table(citiesNoNa[["City", "Race", "Count"]], 
                                 index="City", columns = "Race", values= "Count")
                              .reset_index().reset_index(drop=True).rename_axis(None, axis=1))
    citiesNew = citiesPop.merge(citiesRace, on="City", how="left")    
    
    # to integer 
    citiesNew[["Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born"]] =(citiesNew[["Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born"]].astype(int))
    
    # to float since some rows contain NaN
    # show errors if converting to int
    citiesNew[["American Indian and Alaska Native", "Asian", "Black or African-American", "Hispanic or Latino", "White"]] = (citiesNew[["American Indian and Alaska Native", "Asian", "Black or African-American", "Hispanic or Latino", "White"]].astype(float))    

    # to float
    citiesNew[["Median Age", "Average Household Size"]] = (citiesNew[["Median Age", "Average Household Size"]].astype(float))
    
    # drop duplicates
    citiesNewNoDup = citiesNew.drop_duplicates()
      
    return citiesNewNoDup     
    # insert cities from pandas dataframe
    # citiesNewNoDup.to_sql(table, con=con, if_exists="replace", index=False)
    
def et_staging_immigration(table, sourceFile):
    """
        ET for staging table immigration.
    INPUT: 
        table : table name "immigration"
        sourceFile: source file path for "immigration"
    OUTPUT:
        return pandas data frame
    """
    # read data as "string" format to ensure all the information is correct and drop "Unnamed: 0" column
    immigration = pd.read_csv(sourceFile, dtype="str").drop("Unnamed: 0", axis = 1) 
    
    # drop unnecessary columns
    immigrationNew = immigration.drop(["visapost", "occup", "entdepu", "insnum"], axis=1)
    
    # drop "dtaddto" equal to "D/S" 
    # then can change "dtaddto" to date format since it contain "NaN"
    immigrationPlus = immigrationNew[~(immigrationNew["dtaddto"] == "D/S")]
    
    # change to date from SAS date numeric format
    immigrationPlus["arrdate"] = pd.to_timedelta(immigrationPlus["arrdate"].dropna().astype(float), unit="D") + pd.Timestamp('1960-1-1')
    immigrationPlus["depdate"] = pd.to_timedelta(immigrationPlus["depdate"].dropna().astype(float), unit="D") + pd.Timestamp('1960-1-1')

    # change to date from other formats
    immigrationPlus["dtadfile"] = pd.to_datetime(immigrationPlus["dtadfile"], format="%Y%m%d")
    immigrationPlus["dtaddto"] = pd.to_datetime(immigrationPlus["dtaddto"], format="%m%d%Y")

    # change to int
    immigrationPlus[["i94yr", "i94mon", "i94mode", "i94bir", "i94visa", "count", "biryear"]] = (immigrationPlus[["i94yr", "i94mon", "i94mode", "i94bir", "i94visa", "count", "biryear"]].astype(float).astype(int))
    
    # change to text
    immigrationPlus[["cicid", "i94cit", "i94res", "admnum"]] = (immigrationPlus[["cicid","i94cit", "i94res", "admnum"]].apply(lambda x: x.astype(str).str.replace(".0", "", regex=False), axis=1))

    return immigrationPlus
    # insert tourists from pandas dataframe
    #immigrationPlus.to_sql(table, con=con, if_exists="replace", index=False)
    

def et_staging_temperature(table, sourceFile):
    """
    ET for staging table temperature.
    INPUT: 
        table : table name "temperature"
        sourceFile: source file path for "temperature"
    OUTPUT:
        return pandas data frame
    """
    # read data as "string" format to ensure all the information is correct
    temperature = pd.read_csv(sourceFile, dtype="str") 

    # temperature in 2003 and onwards and in US
    temperatureUS10Yr = temperature[(temperature.Country == "United States") 
                & (temperature.dt >= "2003-01-01")].drop("Country", axis=1)
    
    # drop NaN
    temperatureUS10YrNoNa = temperatureUS10Yr.dropna()     

    # change to date
    temperatureUS10YrNoNa["dt"] = pd.to_datetime(temperatureUS10YrNoNa["dt"], format="%Y-%m-%d")
    
     # to float and round to 1
    temperatureUS10YrNoNa[["AverageTemperature", "AverageTemperatureUncertainty"]] = temperatureUS10YrNoNa[["AverageTemperature", "AverageTemperatureUncertainty"]].astype(float).round(1)
    
    return temperatureUS10YrNoNa

def etl_staging_tables(tables, con):
    """
    Combine all the above defined ET functions together based on the table dictionary input and load the staging tables.
    INPUT:
        tables: dictionary containing the table names and source files
        con: connection object
    OUTPUT:
        None
    """
    for table in tables:
        # staging "airports"
        if table == "airports":
            sourceFile = tables[table]
            airports = et_staging_airports(table, sourceFile)
            airports.to_sql(table, con=con, if_exists="replace", index=False) 
        # staging "immigration"    
        elif table == "immigration":
            sourceFile = tables[table]
            immigration = et_staging_immigration(table, sourceFile)
            immigration.to_sql(table, con=con, if_exists="replace", index=False)  
        # staging "cities"
        elif table == "cities":
            sourceFile = tables[table]
            cities = et_staging_cities(table, sourceFile)
            cities.to_sql(table, con=con, if_exists="replace", index=False) 
        # staging temperature
        else:
            sourceFile = tables[table]
            temperature = et_staging_temperature(table, sourceFile) 
            temperature.to_sql(table, con=con, if_exists="replace", index=False) 
            
            
def create_insert_tables(cur, con):
    """
    Create and insert normalized tables from the staging tables based on the relevant sql queries.
    INPUT:
        cur: cursor object
        con: connection object
    OUTPUT:
        None
    """
    for query in create_and_insert_normalized_table_queries:
        cur.execute(query)
        con.commit()


def main():
    """
    Assign the parameters for the functions and load the functions.
    """
    # create a database called states
    con = sqlite3.connect('states.db')
    cur = con.cursor()
    
    # create a dictionary to store the table names and their source files
    tables = {"airports":"../airport-codes_csv.csv", "immigration":"../immigration_data_sample.csv", "cities":"../us-cities-demographics.csv", "temperature":"../GlobalLandTemperaturesByState.csv"}
    
    # etl function for loading staging tables
    etl_staging_tables(tables, con)

    # create and load normalized tables 
    create_insert_tables(cur, con)

    con.close()


if __name__ == "__main__":
    main()