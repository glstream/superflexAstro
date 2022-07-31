from io import StringIO
import requests, csv, os
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime


@task
def dp_web_scrapper():
    res = requests.get(
        "https://raw.githubusercontent.com/dynastyprocess/data/master/files/values.csv"
    )

    data = res.text

    f = StringIO(data)
    reader = csv.reader(f, delimiter="\n")
    dp_players = []
    enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    next(reader)

    for row in reader:
        prepred_row = [f"{row[0]},{enrty_time}"]
        player_record = [
            i.replace("'", "").replace('"', "").split(",") for i in prepred_row
        ]
        dp_players.extend(player_record)

    dp_players_preped = [
        [
            i[0].split(" ")[0],
            i[0].split(" ")[-1],
            i[0],
            i[11],
            i[1],
            i[2],
            i[3],
            i[4],
            i[5],
            i[6],
            i[7],
            int(i[8]),
            int(i[9]),
            i[12],
        ]
        for i in dp_players
    ]

    return dp_players_preped


@task
def data_validation(dp_players: list):
    return dp_players if len(dp_players) > 0 else False


@task
def dp_player_load(dp_players):

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.dp_player_ranks (
                player_first_name,
                player_last_name,
                player_full_name,
                fp_id,
                player_position,
                team,
                age,
                one_qb_rank_ecr,
                sf_player_ecr_delta,
                ecr_pos,
                one_qb_value,
                sf_value,
                insert_date,
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fp_id)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , position = EXCLUDED.position
            , team = EXCLUDED.team
            , one_qb_rank_ecr = EXCLUDED.one_qb_rank_ecr
            , sf_player_ecr_delta = EXCLUDED.sf_player_ecr_delta
            , ecr_pos = EXCLUDED.ecr_pos
            , one_qb_value = EXCLUDED.one_qb_value
            , sf_value = EXCLUDED.sf_value
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(dp_players),
        page_size=1000,
    )

    print(f"{len(dp_players)} ktc players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.dp_player_ranks"


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

