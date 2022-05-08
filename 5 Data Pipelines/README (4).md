# 1 Introduction
Sparkify plans to use Apache Airflow to introduce more automation and monitoring to their data warehouse ETL pipelines.  
<br >
This project creates high grade, dynamic, and resuable data pipelines which can be monitored and easily backfilled. It also performs data quality check in the end.
<br >
# 2 Getting Stared 
## 2.1 Project Folder
1. "dags" folder contains the "udac_example_dag.py" file configuring the dag for the project.
2. "plugins" folder contains "helpers" and "operators" two sub-folders.
	* "helpers" folder contains all the sql queries for creating staging and normalized tables and inserting into normalized tables based on staging tables created.
    * "operators" folder contains four defined operators: 
    	* StageToRedshiftOperator (stage_redshift.py):
    	* LoadFactOperator (load_fact.py):
    	* LoadDimensionOperator (load_dimension.py):
    	* DataQualityOperator (data_quality.py): 
<br >
## 2.2 How to start the project
1. In the terminal, type "/opt/airflow/start.sh" to start Airflow UI.
2. Connect an IAM user (attach AdministratorAccess, AmazonS3FullAccess, AmazonRedshiftFullAccess) and a Redshift cluster (configure it publicly accessible). 
3. Save all the AWS confidentials.
4. [Connect Airflow to AWS](https://classroom.udacity.com/nanodegrees/nd027/parts/cd0031/modules/1d3d9ce3-4b75-43f9-bc6a-be137e910a38/lessons/ls1969/concepts/891ff961-bf46-43d3-940c-d89a5c723fe5) and [Connect Airflow to Redshift].
5. Turn the Dag on and click "Trigger Dag".
<br >
# 3 About the data
There are two datasets:
* Log data: s3://udacity-dend/log_data
* Song data: s3://udacity-dend/song_data
<br >
# 4 Schema and ETL 
## 4.1 Schema
Use **STAR** schema to create four dimension tables ("artists", "songs", "users", "time") and one fact table ("songpalys").
## 4.2 ETL
The ETL processes are developed by using SQL queries. 
* Extract distinct songs and artists metadata from "staging_songs" to create "artists" and "songs" separately.
* Extract distinct user metadata from "staging_events" and filter the records with "page" equal to "NextSong" to create "users".
* Join "staging_songs" and "staging_events" and create "start_time" column by converting milliseconds to timestamp and filter the records with "page" equal to "NextSong" to create "songplays".
* Extract different levels of time like year, month, week, day of week, etc from the "start_time" column created in "songplays".
