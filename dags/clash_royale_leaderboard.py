import datetime
import pendulum
import os
import logging

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
                level SMALLINT CHECK (level > 0),
                score SMALLINT CHECK (score > 0),
                rank SMALLINT CHECK (rank > 0),
                clan JSONB,
                ingestion_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """
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
            clan JSONB,
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
            "eloRating": "score"
        }

        df = (
            get_data(url)
            .rename(rename_cols)
            .select("tag", "name", "level", "score", "rank", "clan")
            .with_columns(
                clan = pl.col("clan").struct.json_encode(),
                ingestion_timestamp = datetime.datetime.now()
            )
        )

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_uri()

        df.write_database(
            table_name="path_of_legend_temp",
            connection=conn,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

    @task
    def fetch_and_write_merge_tactics_date():
        url = BASE_URL + f"leaderboard/743200?limit=10000" 

        df = (
            get_data(url)
            .select("tag", "name", "rank", "score", "clan")
            .with_columns(
                clan = pl.col("clan").struct.json_encode(),
                ingestion_timestamp = datetime.datetime.now()
            )
        )

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_uri()

        df.write_database(
            table_name="merge_tactics_temp",
            connection=conn,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

    @task
    def upsert_path_of_legend():
        query = """
            MERGE INTO path_of_legend t
            USING (SELECT DISTINCT * FROM path_of_legend_temp) s
            ON t.tag = s.tag
            WHEN MATCHED THEN UPDATE SET
                name = s.name,
                level = s.level,
                score = s.score,
                rank = s.rank,
                clan = s.clan::jsonb,
                ingestion_timestamp = s.ingestion_timestamp
            WHEN NOT MATCHED THEN
                INSERT (tag, name, level, score, rank, clan, ingestion_timestamp)
                VALUES (s.tag, s.name, s.level, s.score, s.rank, s.clan::jsonb, s.ingestion_timestamp)
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            logging.error(f"Task failed: {e}")
            return 1

    @task
    def upsert_merge_tactics():
        query = """
            MERGE INTO merge_tactics t
            USING (SELECT DISTINCT * FROM merge_tactics_temp) s
            ON t.tag = s.tag
            WHEN MATCHED THEN UPDATE SET
                name = s.name,
                rank = s.rank,
                score = s.score,
                clan = s.clan::jsonb,
                ingestion_timestamp = s.ingestion_timestamp
            WHEN NOT MATCHED THEN
                INSERT (tag, name, rank, score, clan, ingestion_timestamp)
                VALUES (s.tag, s.name, s.rank, s.score, s.clan::jsonb, s.ingestion_timestamp)
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            logging.error(f"Task failed: {e}")
            return 1

    create_player_ranking_table = SQLExecuteQueryOperator(
        task_id = "create_player_ranking_table",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS player_ranking;
            CREATE TABLE player_ranking AS
            SELECT
                p.tag,
                p.name,
                p.clan ->> 'name' clan_name,
                p.rank path_of_legend_rank,
                m.rank merge_tactics_rank
            FROM path_of_legend p
            INNER JOIN merge_tactics m
            ON p.tag = m.tag
            ORDER BY path_of_legend_rank DESC
        """
    )

    create_top_clan_table = SQLExecuteQueryOperator(
        task_id = "create_top_clan_table",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS top_clan;
            CREATE TABLE top_clan AS
            SELECT
                p.clan ->> 'name' clan_name,
                COUNT(DISTINCT p.tag) player_count
            FROM path_of_legend p
            FULL JOIN merge_tactics m
            ON p.tag = m.tag
            GROUP BY clan_name
            ORDER BY player_count DESC
            LIMIT 100
        """
    )

    upsert_path_of_legend = upsert_path_of_legend()
    upsert_merge_tactics = upsert_merge_tactics()
    create_path_of_legend_table >> fetch_and_write_path_of_legend_data() >> upsert_path_of_legend
    create_merge_tactics_table >> fetch_and_write_merge_tactics_date() >> upsert_merge_tactics
    [upsert_path_of_legend, upsert_merge_tactics] >> create_player_ranking_table
    [upsert_path_of_legend, upsert_merge_tactics] >> create_top_clan_table


dag = process_clash_royale_leaderboard()