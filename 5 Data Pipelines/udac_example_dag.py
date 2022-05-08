from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Define the default parameters for DAG as instructed
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12)#,
#     'depends_on_past': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
#     'catchup': False,
#     'email_on_retry': False
}

# Create DAG instance based on the default DAG
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create and copy staging_events from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    sql_create_staging=SqlQueries.staging_events_table_create
)

# Create and copy staging_songs from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    sql_create_staging=SqlQueries.staging_songs_table_create
)

# Create and load Fact songplays based on the staging tables created
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    sql_create_normalized=SqlQueries.songplay_table_create,
    sql_insert_normalized=SqlQueries.songplay_table_insert
    
)

# Create and load Dimension users based on the staging tables created
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql_create_normalized=SqlQueries.user_table_create,
    sql_insert_normalized=SqlQueries.user_table_insert
)

# Create and load Dimension songs based on the staging tables created
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql_create_normalized=SqlQueries.song_table_create,
    sql_insert_normalized=SqlQueries.song_table_insert
)

# Create and load Dimension artists based on the staging tables created
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql_create_normalized=SqlQueries.artist_table_create,
    sql_insert_normalized=SqlQueries.artist_table_insert
)

# Create and load Dimension time based on the staging tables created
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    sql_create_normalized=SqlQueries.time_table_create,
    sql_insert_normalized=SqlQueries.time_table_insert
)

# Data quality check for all the tables created 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["staging_events", "staging_songs", "songplays", "users", "artists", "songs", "time"]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG diagram
# begin & staging
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# fact tables
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# normalized tables
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_time_dimension_table

# quality check
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# end
run_quality_checks >> end_operator