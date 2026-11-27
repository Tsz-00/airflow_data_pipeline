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
    start_date=pendulum.datetime(2026, 11, 26, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_clash_royale_leaderboard():
    create_leaderboard_table = SQLExecuteQueryOperator(
        task_id="create_leaderboard_table",
        conn_id="tutorial_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS clash_royale_leaderboard (
                "tag" VARCHAR(15) PRIMARY KEY,
                "name" TEXT NOT NULL,
                "eloRating" SMALLINT NOT NULL,
                "rank" SMALLINT NOT NULL,
                "ingestion_timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""",
    )

    create_leaderboard_temp_table = SQLExecuteQueryOperator(
        task_id="create_leaderboard_temp_table",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS clash_royale_leaderboard_temp;
            CREATE TABLE IF NOT EXISTS clash_royale_leaderboard_temp (
                "tag" VARCHAR(15) PRIMARY KEY,
                "name" TEXT NOT NULL,
                "eloRating" SMALLINT NOT NULL,
                "rank" SMALLINT NOT NULL,
                "ingestion_timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );""",
    )

    @task
    def get_data():
        #DATA_DIR = "/opt/airflow/dags/files/clash_royale_leaderboard.parquet"
        URL = "https://api.clashroyale.com/v1/locations/global/pathoflegend/players?limit=10000"
        headers = {
            "Authorization": f"Bearer {os.getenv("CLASH_ROYALE_API_TOKEN")}",
            "Content-Type": "application/json"
        }

        #os.makedirs(os.path.dirname(DATA_DIR), exist_ok=True)

        try:
            response = requests.get(URL, headers=headers, timeout=30)
        except Exception as e:
            print(e)
        else:
            df = (
                pl.DataFrame(json.loads(response.content).get("items"))
                .select("tag", "name", "eloRating", "rank")
                .with_columns(
                    ingestion_timestamp = datetime.datetime.now()
                )
            )
            #df.write_parquet(DATA_DIR)

            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_uri()

            df.write_database(
                table_name="clash_royale_leaderboard_temp",
                connection=conn,
                if_table_exists="replace",
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

    [create_leaderboard_table, create_leaderboard_temp_table] >> get_data() >> merge_data()


dag = process_clash_royale_leaderboard()