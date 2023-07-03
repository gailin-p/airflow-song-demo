from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
import sql_queries
from airflow.models import Variable


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    "default_task_retries":3,
    "retry_delay":timedelta(minutes=5),
    "catchup":False,
    "email_on_retry":False,
    "depends_on_past":False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
)
def final_project():

    queries = sql_queries.SqlQueries

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        redshift_conn_id="aws_credentials",
        task_id='Stage_events',
        table_name="public.staging_events",
        s3_path=Variable.get("log_data_path"),
        create_table_sql = queries.staging_events_table_create,
        iam_role = Variable.get("iam_role")
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table_name="public.staging_songs",
        s3_path=Variable.get("song_path"),
        create_table_sql = queries.staging_songs_table_create,
        iam_role = Variable.get("iam_role")
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        sql_load_statement = queries.songplay_table_insert,
        sql_create_statement = queries.create_songplays,
        table_name="public.songplays",
        truncate=True,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_load_statement = queries.user_table_insert,
        sql_create_statement = queries.create_users,
        table_name="public.users",
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_load_statement = queries.song_table_insert,
        sql_create_statement = queries.create_songs,
        table_name="public.songs",
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_load_statement = queries.artist_table_insert,
        sql_create_statement = queries.create_artist,
        table_name="public.artists",
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_load_statement = queries.time_table_insert,
        sql_create_statement = queries.create_times,
        table_name='public."time"',
        truncate=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        table_list = ["public.songplays", "public.users"],
        column_list=["playid","userid"],
        max_allowed=0,
    )

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

final_project_dag = final_project()