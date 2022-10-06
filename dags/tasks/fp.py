import requests
from bs4 import BeautifulSoup
import re
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime


@task()
def fp_proj_web_scraper():
    enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    nfl_week = requests.get("https://api.sleeper.app/v1/state/nfl").json()["leg"]
    positions = ["qb", "rb", "wr", "te"]
    all_players = []
    for pos in positions:
        position_url = f"https://www.fantasypros.com/nfl/projections/{pos}.php?week={nfl_week}&scoring=PPR"
        proj = requests.get(position_url)
        soup = BeautifulSoup(proj.text, "html.parser")
        proj_score = soup.find_all("td", attrs={"data-sort-value": True})
        names = soup.find_all("a", class_={"player-name": True})
        players = [
            [
                names[i]
                .text.replace("'", "")
                .replace('"', "")
                .replace(" III", "")
                .replace(" II", "")
                # .replace("Gabriel", "Gabe")
                .replace(" Jr.", ""),
                pos.upper(),
                proj_score[i].text,
            ]
            for i in range(len(names))
        ]
        all_players.extend(players)
    fp_players_preped = [
        [
            i[0].split(" ")[0],  # firstname
            i[0].split(" ")[-1],  # last_name
            i[0],  # fullname
            int(float(i[2])),
            enrty_time,
        ]
        for i in all_players
    ]
    return fp_players_preped


@task()
def data_validation(fp_players_preped: list):
    return fp_players_preped if len(fp_players_preped) > 0 else False


@task()
def fp_player_load(fp_players_preped: list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.fp_player_projections (
                player_first_name,
                player_last_name,
                player_full_name,
                total_projection,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , total_projection = EXCLUDED.total_projection
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(fp_players_preped),
        page_size=1000,
    )

    print(f"{len(fp_players_preped)} nfl players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.fp_player_projections"


@task()
def surrogate_key_formatting(table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    print("Connection established")
    cursor.execute(
        f"""UPDATE {table_name} 
                        SET player_first_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(player_first_name,'.',''), ' Jr', ''), ' III',''),'Jeffery','Jeff'), 'Joshua','Josh'),'William','Will'), ' II', ''),'''',''),'Kenneth','Ken'),'Mitchell','Mitch'),'DWayne','Dee')
                        """
    )
    conn.commit()
    cursor.close()
    return
