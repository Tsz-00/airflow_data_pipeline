import datetime
import pendulum
import os

import polars as pl
import json
from dotenv import load_dotenv

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

load_dotenv()

@dag(
    dag_id="clash_royale_leaderboard",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 10, 20, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_clash_royale_leaderboard():
    BASE_URL = "https://api.clashroyale.com/v1/"
    headers = {
        "Authorization": f"Bearer {os.getenv("CLASH_ROYALE_API_TOKEN")}",
        "Content-Type": "application/json"
    }

    create_path_of_legend_table = SQLExecuteQueryOperator(
        task_id="create_path_of_legend_table",
        conn_id="tutorial_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS path_of_legend (
                tag VARCHAR(15) PRIMARY KEY,
                name TEXT NOT NULL,
                level SMALLINT CHECK (level > 0)
                score SMALLINT CHECK (score > 0),
                rank SMALLINT CHECK (rank > 0),
                clan JSONB,
                ingestion_timestamp TIMESTAMPTZ DEFAULT NOW()
            );""",
    )

    create_merge_tactics_table = SQLExecuteQueryOperator(
        task_id = "create_merge_tactics_table",
        conn_id="tutorial_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS merge_tactics (
            tag VARCHAR(15) PRIMARY KEY,
            name TEXT NOT NULL,
            rank SMALLINT CHECK (rank > 0),
            score SMALLINT CHECK (score > 0),
            clan JSONB
            ingestion_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
            """
    )

    def get_data(url) -> pl.DataFrame:
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except Exception as e:
            print(e)
            return pl.DataFrame()
        else:
            df = pl.DataFrame(json.loads(response.content).get("items"))
            return df

    @task
    def fetch_and_write_path_of_legend_data():
        url = BASE_URL + "locations/global/pathoflegend/players?limit=1000"
        rename_cols = {
            "expLevel": "level",
            "eloRatin": "score"
        }

        df = (
            get_data(url)
            .rename(rename_cols)
            .select("tag", "name", "level", "score", "rank", "clan")
            .with_columns(
                clan = pl.col("clan").struct.json_encode()
            )
        )

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_uri()

        df.write_database(
            table_name="path_of_legend",
            connection=conn,
            if_table_exists="append",
            engine="sqlalchemy",
        )

    @task
    def fetch_and_write_merge_tactics_date():
        url = BASE_URL + f"leaderboard/743200?limit=1000" 

        df = (
            get_data(url)
            .select("tag", "name", "rank", "score", "clan")
            .with_columns(
                clan = pl.col("clan").struct.json_encode()
            )
        )

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_uri()

        df.write_database(
            table_name="merge_tactics",
            connection=conn,
            if_table_exists="append",
            engine="sqlalchemy",
        )
            
    @task
    def merge_data():
        query = """
            MERGE INTO clash_royale_leaderboard t
            USING (SELECT DISTINCT * FROM clash_royale_leaderboard_temp) s
            ON t.tag = s.tag
            WHEN MATCHED THEN UPDATE SET
                name = s.name,
                eloRating = s.eloRating
                rank = s.rank
                ingestion_timestamp = s.ingestion_timestamp
            WHEN NOT MATCHED THEN
                INSERT (tag, name, eloRating, rank, ingestion_timestamp)
                VALUES (s.tag, s.name, s.eloRating, s.rank, s.ingestion_timestamp))
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_leaderboard_table] >> get_data() >> merge_data()


dag = process_clash_royale_leaderboard()