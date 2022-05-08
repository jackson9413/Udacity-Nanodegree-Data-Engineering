# 1 Introduction #
This project is developed to build a data warehouse store the data related to US immigrants, temperature, airports, and population in each of the state and perform data analysis based on the normalized tables created by using STAR schema from all the files sourced publicly.   
<br >
It includes two parts: "On Premise" and "AWS Cloud" 
* "On Premise":
    * It uses Python, SQL to build ETL pipelines and deploy a data warehouse by using SQLite database.
* "AWS Cloud":
    * It uses Python, SQL, and PySpark to build ETL pipelines. 
    * And use AWS S3 bucket to store all the staging files and copy staging files from S3 to Redshift to creat staging tables and normalized tables.
<br >
# 2 Getting Started #
Go to "On Premise" and "AWS Cloud" folders and refer to the relevant "README.md" for introduction and how to get started. 
* "On Premise" contains all the files and "README.md" to create a on-premise data warehouse.
* "AWS Cloud" contains all the files and "README.md" to create an AWS Redshift data warehouse.
* "sas_data":  **I94 Immigration Data** (This data comes from the US National Tourism and Trade Office). 
* "airport-codes_csv.csv": **Airport Code Table** (This is a simple table of airport codes and corresponding cities).
* "GlobalLandTemperaturesByState.csv": **World Temperature Data** (This dataset came from Kaggle).
* "I94_SAS_Labels_Descriptions.SAS": metadata for **I94 Immigration Data**.
* "immigration_data_sample.csv": a sample file for **I94 Immigration Data**.
* "us-cities-demographics.csv": population demographics of US cities.
