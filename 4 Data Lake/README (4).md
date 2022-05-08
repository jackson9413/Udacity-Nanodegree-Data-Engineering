# 1 Introduction
Sparkify wants to move their data warehouse to a data lake where data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in thier app.
<br />
The project builds an ETL pipeline which extracts the data from S3, processes them using Spark, and loads the data back to S3 as a set of dimensional tables.
<br />
# 2 Getting Started
* 2.1 Copy AWS access and secret access key and fill them in the "dl.cfg".
* 2.2 Launch AWS console and create an AWS S3 bucket.
* 2.3 Launch an AWS EMR cluster and create a new notebook to run the ETL codes. 
* 2.4 Deploy the ETL codes from notebook to "etl.py".
* 2.5 Run the "etl.py" in a terminal by typing "python etl.py".
* 2.6 Check the S3 bucket.
<br />
# 3 About the Datasets
The two datasets reside in S3:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data  
Log data json path: s3://udacity-dend/log_json_path.json 
<br />
# 4 Database Schema
* Fact Table
**songplays** - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* Dimension Table
    * **users** - users in the app: user_id, first_name, last_name, gender, level
    * **songs** - songs in music database: song_id, title, artist_id, year, duration
    * **artists - artists in music database: artist_id, name, location, lattitude, longitude**
    * **time** - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday
<br />
# 5 ELT 
1. Read the "song_data" and "log_data" json files as pandas dataframes by using spark.
2. Extract the relevant columns from the dataframe created by "song_data" json files to create "songs_table" and "artists_table" and drop the duplicates and save them as parquet files in S3 bucket created.
3. Filter the dataframe created by "log_data" json files with "page" equal to "NextSong". 
4. Extract the relevent columns from the above filterred dataframe and drop the duplicates to create "users_table" as well as saving it as a parquet file.
5. Create a new column "start_time" in the filterred dataframe (Step 3.) by converting the milliseconds column "ts" to a timestamp column.
6. Extrac the hour, week, month, year, weekday from the "start_time" column as well as "time_id" to create "time_table".
7. For "songplays_table", first join the "songs_table" and "artists_table" to get the "song_id", "artist_id", "title", "duration", "artist_name" in one dataframe. Then, join the dataframe (Step 5.) and extract the relevant columns to create "songplays_table".

