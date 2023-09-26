# Importing defaults for dag
from datetime import datetime, timedelta
import os
from airflow.decorators import dag,task

# Importing hooks
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

# Importing operators
from airflow.operators.dummy_operator import DummyOperator
from custom_operators.create_schema import CreateSchemaOperator
from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator

# Importing SQL queries
from sql.final_project_sql_statements import sqlqueries
from sql.final_project_sql_schema import sqlschema

# default parameters for DAG
default_args = {
    'owner': 'melissa',
    'start_date': datetime(2023, 9, 19),
    ##'depends_on_past': False,
    ##'retries': 3,
    ##'retry_delay': timedelta(minutes=5),
    ##'catchup': False,
    ##'email_on_retry': False
}

# DAG hourly schedule and monitoring
@dag(
    default_args=default_args,
    schedule_interval='@hourly',
    description='Load and transform data in Redshift with Airflow',
    catchup=False,
    max_active_runs=1
)

# TASKS for DAG and DAG name final_project
def final_project():
    @task
    def start_execution():
        return 'start_execution'

    create_schema = CreateSchemaOperator(
        task_id="Create_schema",
        redshift_conn_id="redshift",
        sql=sqlschema.create,
        skip=False
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id='aws_credentials',
        redshift_conn_id='redshift',
        region='us-west-2',
        table='staging_events',
        s3_bucket='melissa-airflow',
        s3_key='log-data',
        s3_format='s3://melissa-airflow/log-data/log_json_path.json',


    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credentials_id='aws_credentials',
        redshift_conn_id='redshift',
        region='us-west-2',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song-data/A/B/C',
        s3_format='auto'
    )

    load_songplays = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=sqlqueries.songplay_table_insert
    )

    load_users = LoadDimensionOperator(
        task_id='Load_users_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=sqlqueries.user_table_insert,
        insert_mode='with truncate'
    )

    load_songs = LoadDimensionOperator(
        task_id='Load_songs_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=sqlqueries.song_table_insert,
        insert_mode='with truncate'
    )

    load_artists = LoadDimensionOperator(
        task_id='Load_artists_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=sqlqueries.artist_table_insert,
        insert_mode='with truncate'
    )

    load_time = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=sqlqueries.time_table_insert,
        insert_mode='with truncate'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    @task
    def end_execution():
        return 'End_execution'

    start_execution = start_execution()
    end_execution = end_execution()

    start_execution >> create_schema
    create_schema >> stage_events_to_redshift >> load_songplays
    create_schema >> stage_songs_to_redshift >> load_songplays
    load_songplays >> load_users >> run_quality_checks
    load_songplays >> load_songs >> run_quality_checks
    load_songplays >> load_artists >> run_quality_checks
    load_songplays >> load_time >> run_quality_checks
    run_quality_checks >> end_execution

final_project_dag = final_project()