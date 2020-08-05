from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udacity_dend_proj_5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Load staging events data to redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2"
)

#Load staging songs data to redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table