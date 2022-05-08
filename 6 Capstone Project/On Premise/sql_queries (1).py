# DROP TABLES

# staging
staging_cities_table_drop = "DROP TABLE IF EXISTS cities;"
staging_airports_table_drop = "DROP TABLE IF EXISTS airports;"
staging_immigration_table_drop = "DROP TABLE IF EXISTS immigration;"
staging_temperature_table_drop = "DROP TABLE IF EXISTS temperature;"

# normalized 
dim_airports_table_drop = "DROP TABLE IF EXISTS dim_airports;"
dim_cities_table_drop = "DROP TABLE IF EXISTS dim_cities;"
dim_temperature_table_drop = "DROP TABLE IF EXISTS dim_temperature;"
fact_immigration_table_drop = "DROP TABLE IF EXISTS fact_immigration;"


# CREATE TABLES

# staging
staging_airports_table_create= ("""CREATE TABLE IF NOT EXISTS airports
                                   (airport_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                   ident TEXT NOT NULL, 
                                   type TEXT NOT NULL, 
                                   name TEXT NOT NULL,
                                   elevation_ft REAL NOT NULL, 
                                   region TEXT NOT NULL,
                                   longitude REAL NOT NULL,
                                   latitude REAL NOT NULL);
               """)

staging_cities_table_create = ("""CREATE TABLE IF NOT EXISTS cities 
                                   (city_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                   City TEXT NOT NULL, 
                                   State TEXT NOT NULL, 
                                   "Median Age" REAL NOT NULL,
                                   "Male Population" INTEGER NOT NULL, 
                                   "Female Population" INTEGER NOT NULL,
                                   "Total Population" INTEGER NOT NULL,
                                   "Number of Veterans" INTEGER NOT NULL,
                                   "Foreign-born" INTEGER NOT NULL,
                                   "Average Household Size" REAL NOT NULL,
                                   "State Code" TEXT NOT NULL,
                                   "American Indian and Alaska Native" INTEGER,
                                   "Asian" INTEGER,
                                   "Black or African-American" INTEGER,
                                   "Hispanic or Latino" INTEGER NOT NULL,
                                   "White" INTEGER NOT NULL
                                   );
""")


staging_immigration_table_create= ("""CREATE TABLE IF NOT EXISTS immigration
                                       (cicid INTEGER PRIMARY KEY NOT NULL,
                                       i94yr INTEGER NOT NULL, 
                                       i94mon INTEGER NOT NULL, 
                                       i94cit TEXT NOT NULL,
                                       i94res TEXT NOT NULL,
                                       i94port TEXT NOT NULL,
                                       arrdate TEXT NOT NULL,
                                       i94mode INTEGER NOT NULL,
                                       i94addr TEXT,
                                       depdate TEXT,
                                       i94bir INTEGER NOT NULL,
                                       i94visa INTEGER NOT NULL,
                                       count INTEGER NOT NULL,
                                       dtadfile TEXT NOT NULL,
                                       visapost TEXT,
                                       entdepa TEXT NOT NULL,
                                       entdepd TEXT,
                                       matflag TEXT,
                                       biryear INTEGER NOT NULL,
                                       dtaddto TEXT NOT NULL,
                                       gender TEXT,
                                       airline TEXT,
                                       admnum TEXT NOT NULL,
                                       fltno TEXT,
                                       visatype TEXT NOT NULL);
               """)

staging_temperature_table_create = ("""CREATE TABLE IF NOT EXISTS temperature
                                       (temp_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                       dt TEXT NOT NULL,
                                       AverageTemperature REAL NOT NULL,
                                       AverageTemperatureUncertainty REAL NOT NULL,
                                       State TEXT NOT NULL
                                       );
""")


# normalized tables

dim_airports_table_create = (""" CREATE TABLE IF NOT EXISTS dim_airports AS
                SELECT DISTINCT tb.region,  totalAirport FROM
                    (SELECT region, COUNT(ident) AS totalAirport
                    FROM airports
                    GROUP BY 1) tb
                    WHERE region IN (SELECT DISTINCT [State Code] FROM cities)
                    AND region IN (SELECT DISTINCT i94addr FROM immigration);
""")

dim_cities_table_create = (""" CREATE TABLE IF NOT EXISTS dim_cities AS
                SELECT DISTINCT tb.* FROM
                    (SELECT [State Code], 
                    ROUND(AVG([Median Age]), 1) AS [Median Age],
                    SUM([Male Population]) AS [Male Population],
                    SUM([Female Population]) AS [Female Population],
                    SUM([Total Population]) AS [Total Population],
                    SUM([Number of Veterans]) AS [Number of Veterans],
                    SUM([Foreign-born]) AS [Foreign-born],
                    ROUND(AVG([Average Household Size]), 1) AS [Average Household Size],
                    ROUND(SUM([American Indian and Alaska Native]), 1) AS [American Indian and Alaska Native],
                    ROUND(SUM([Asian]), 1) AS [Asian],
                    ROUND(SUM([Black or African-American]), 1) AS [Black or African-American],
                    ROUND(SUM([Hispanic or Latino]), 1) AS [Hispanic or Latino],
                    ROUND(SUM([White]), 1) AS [White] FROM cities
                    GROUP BY [State Code]) tb
                    WHERE [State Code] IN (SELECT DISTINCT region FROM airports)
                    AND [State Code] IN (SELECT DISTINCT i94addr FROM immigration);
""")

dim_temperature_table_create = (""" CREATE TABLE IF NOT EXISTS dim_temperature AS
                WITH season AS (
                SELECT 
                    CASE 
                    WHEN strftime('%m', dt) IN ('03', '04', '05') THEN 'Spring'
                    WHEN strftime('%m', dt) IN ('06', '07', '08') THEN 'Summer'
                    WHEN strftime('%m', dt) IN ('09', '10', '11') THEN 'Fall'
                    WHEN strftime('%m', dt) IN ('12', '01', '02') THEN 'Winter'
                    END Season, 
                    CASE 
                    WHEN State = 'Georgia (State)' THEN 'Georgia'
                    WHEN State = 'District Of Columbia' THEN 'District of Columbia'
                    ELSE State
                    END AS State
                    , 
                    ROUND(AVG(AverageTemperature), 1) AS AverageTemperature 
                    FROM temperature
                    GROUP BY 1, 2
                )
                SELECT DISTINCT cities.[State Code], FallAvgTemp, SummerAvgTemp, SpringAvgTemp, WinterAvgTemp FROM
                    (SELECT State, 
                    CASE WHEN Season = 'Fall' THEN AverageTemperature END AS FallAvgTemp FROM season) fall 
                    JOIN (
                    SELECT State, 
                    CASE WHEN Season = 'Summer' THEN AverageTemperature END AS SummerAvgTemp FROM season 
                    ) summer
                    ON fall.State = Summer.State
                    JOIN (
                    SELECT State, 
                    CASE WHEN Season = 'Spring' THEN AverageTemperature END AS SpringAvgTemp FROM season     
                    ) spring
                    ON fall.State = spring.State
                    JOIN (
                    SELECT State, 
                    CASE WHEN Season = 'Winter' THEN AverageTemperature END AS WinterAvgTemp FROM season        
                    ) winter
                    ON fall.State = winter.State
                    JOIN cities 
                    ON fall.State = cities.State
                    WHERE cities.[State Code] IN (SELECT DISTINCT region FROM airports)
                    AND cities.[State Code] IN (SELECT DISTINCT i94addr FROM immigration)
                    AND FallAvgTemp IS NOT NULL AND SummerAvgTemp IS NOT NULL AND SpringAvgTemp IS NOT NULL AND WinterAvgTemp IS NOT NULL;
""")


fact_immigration_table_create = ("""CREATE TABLE IF NOT EXISTS fact_immigration AS
                    SELECT * FROM 
                    immigration  
                    WHERE i94addr IN
                    (SELECT DISTINCT [State Code] FROM cities)
                    AND i94addr IN (SELECT DISTINCT region FROM airports);
                """)


# QUERY LISTS

drop_table_queries = [staging_cities_table_drop, staging_airports_table_drop, staging_immigration_table_drop, staging_temperature_table_drop, dim_airports_table_drop, dim_cities_table_drop, dim_temperature_table_drop, fact_immigration_table_drop]

create_staging_table_queries = [staging_airports_table_create, staging_cities_table_create, staging_immigration_table_create, staging_temperature_table_create]

create_and_insert_normalized_table_queries = [dim_airports_table_create, dim_cities_table_create, dim_temperature_table_create, fact_immigration_table_create]