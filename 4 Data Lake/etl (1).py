# import the packages
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, dayofweek, row_number, monotonically_increasing_id


# environment configurations
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    create a spark session and return it
    """
    spark = (SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate())
    return spark


def process_song_data(spark, input_data, output_data):
    """
    read "song_data" json files and create "songs_table" and "artists_table" and save them as parquet files by using PySpark.
    INPUT:
    spark: spark session
    input_data: s3 folder path storing data
    output_data: s3 folder path storing the parquet files
    RETURN: None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    #df = spark.read.json("data/song/song_data/*/*/*/*.json")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (df.select(["song_id", "title", "artist_id", "year", "duration"])\
        .dropDuplicates(subset=["song_id"]))
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table.write.partitionBy("year", "artist_id")
        .mode("overwrite")
        .parquet(output_data + "songs.parquet"))

    # extract columns to create artists table
    artists_table = (df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])\
        .dropDuplicates(subset=["artist_id"]))
    
    # write artists table to parquet files
    (artists_table.write
        .mode("overwrite")
        .parquet(output_data + "artists.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    read "log_data" json files to create "uers_table" and "time_table" and "songplays_table" and save them as parquet files.
    INPUT:
    spark: spark session
    input_data: s3 folder path storing data
    output_data: s3 folder path storing the parquet files
    RETURN: None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    #df = spark.read.json("data/log/*.json")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = (df.select(["userId", "firstName", "lastName", "gender", "level"])\
        .dropDuplicates(subset=["userId"]))
    
    # write users table to parquet files
    (users_table.write
        .mode("overwrite")
        .parquet(output_data + "users.parquet"))
        
    # use "to_timestamp" to convert milliseconds to timestamp 
    df = df.withColumn("start_time", to_timestamp(col("ts") / 1000.0))
    
    # extract columns to create time table
    time_table = (df.withColumn("hour", hour(col("start_time")))\
        .withColumn("day", dayofmonth(col("start_time")))\
        .withColumn("week", weekofyear(col("start_time")))\
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("weekday", dayofweek(col("start_time")))\
        .select(["start_time", "hour", "day", "week", "month", "year", "weekday"])\
        .dropDuplicates(subset=["start_time"])\
        .withColumn("time_id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)\
        .select(["time_id", "start_time", "hour", "day", "week", "month", "year", "weekday"]))
    
    # write time table to parquet files partitioned by year and month
    (time_table.write.partitionBy("year", "month")
        .mode("overwrite")
        .parquet(output_data + "time.parquet"))
    
    # read in song data to use for songplays table
    song_df = (songs_table.alias("s").join(artists_table.alias("a"), col("s.artist_id") == col("a.artist_id"), "inner")\
        .select(col("s.song_id"), col("s.title"), col("s.artist_id"), col("s.duration"), col("a.artist_name"))\
        .dropDuplicates(subset=["song_id", "artist_id"]))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (df.join(song_df, [df.song == song_df.title, df.length == song_df.duration, df.artist == song_df.artist_name], "inner")\
        .select(["start_time", "year", "month", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"])\
        .withColumn("songplay_id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)\
        .select(["songplay_id", "start_time", "year", "month", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]))

    # write songplays table to parquet files partitioned by year and month
    (songplays_table.write.partitionBy("year", "month")
        .mode("overwrite")
        .parquet(output_data + "songplays_table.parquet"))

def main():
    """
    "main" function combines the above defined functions together and assign the values for variables
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = "data"
    output_data = "s3a://udacity-data-lake-self/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
