# airflow_data_pipeline
This project aim to produce gaming (Clash Royale) ETL data pipeline by leveraging Apache Airflow, Docker Compose, PostgreSQL, Python, Gaming API technique.

- Designed and orchestrated an Airflow DAG (running on Docker Compose) to extract player and event data from gaming event APIs
- Implemented incremental upsert logic to load and update data into PostgreSQL, ensuring data consistency across runs
- Developed a downstream task to identify and calculate common players who participated in both gaming events

## Architecture
- `Extract`  Gaming API  
- `Transform`  Staging table → Deduplicates → Upsert  
- `Load`  Postgres Database
- `Aggregated Tables` Player ranking & Top clan
<img width="1828" height="494" alt="image" src="https://github.com/user-attachments/assets/f2ae56d4-7623-4dd5-a957-624c5f7af538" />
<img width="2736" height="914" alt="image" src="https://github.com/user-attachments/assets/f08d4941-8731-4c2d-b5eb-a8ef67db0fbd" />
** Path of Legend and Merge Tactics are two events in this Game

## `Extract`
- Clash Royale API return JSON items
- Path of Legend event return Top 1000 players
- Merge Tactics event return Top 10000 players
<img width="1378" height="1182" alt="image" src="https://github.com/user-attachments/assets/fbb71c94-d534-451a-a745-788a9a8c2092" />

## `Transform`  `Load`
- Land data in temperory table in Postgre Database (`path_of_legend_temp` & `merge_tactics_temp`)
- Deduplicate and merge in `path_of_legend` & `merge_tactics` for unique records
<img width="2460" height="1488" alt="image" src="https://github.com/user-attachments/assets/ad498382-38f3-4d53-85e3-5377715499a2" />

## Aggregate result
- player_ranking (top players in both event)
- top_clan (clan with most top players)
<img width="970" height="1158" alt="image" src="https://github.com/user-attachments/assets/01da4033-06bc-46f7-8959-d9c05f4d327f" />
