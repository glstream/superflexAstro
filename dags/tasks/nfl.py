import requests
from bs4 import BeautifulSoup
import re
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime


@task()
def nfl_web_scrapper() -> list:
    nfl_projections_players = []
    for i in range(1, 846, 25):
        offset = str(i)
        base_projections = f"https://fantasy.nfl.com/research/projections?offset={offset}&position=O&sort=projectedPts&statCategory=projectedStats&statSeason=2022&statType=seasonProjectedStats"
        res = requests.get(base_projections)

        soup = BeautifulSoup(res.text, "html.parser")
        enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

        projections = soup.find_all(
            "td", {"class": "stat projected numeric sorted last"}
        )
        names = soup.find_all("td", {"class": "playerNameAndInfo first"})
        players = [
            [
                names[i]
                .find("a")
                .get_text()
                .replace("'", "")
                .replace('"', "")
                .replace(" III", "")
                .replace(" II", "")
                .replace("Gabe", "Gabriel")
                .replace(" Jr.", ""),
                re.split("=", names[i].find("a")["href"])[-1],
                names[i].find("a")["href"],
                str(projections[i])[47:-5]
            ]
            for i in range(len(names))
        ]
        nfl_projections_players.extend(players)
    nfl_players_preped = [
        [
            i[0].split(" ")[0], #firstname
            i[0].split(" ")[-1], #last_name
            i[0], #fullname
            i[1], #nfl_player_id
            i[2], #slug
            int(float(i[3])),
            enrty_time
        ]
        for i in nfl_projections_players
    ]    

    return nfl_players_preped


@task()
def data_validation(nfl_players_preped: list):
    return nfl_players_preped if len(nfl_players_preped) > 0 else False


@task()
def nfl_player_load(nfl_players_preped: list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.nfl_player_projections (
                player_first_name,
                player_last_name,
                player_full_name,
                nfl_player_id,
                slug,
                total_projection,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (nfl_player_id)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , player_full_name = EXCLUDED.player_full_name
            , slug = EXCLUDED.slug
            , total_projection = EXCLUDED.total_projection
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(nfl_players_preped),
        page_size=1000,
    )

    print(f"{len(nfl_players_preped)} nfl players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.nfl_player_projections"


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

