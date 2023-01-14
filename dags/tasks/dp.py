from io import StringIO
import requests, csv
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime


@task()
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
            i.replace("'", "").replace('"', "").replace(' III', "").replace(' II', "").replace(' Jr.', "").split(",") for i in prepred_row
        ]
        dp_players.extend(player_record)

    dp_players_preped = [
        [
            i[0].split(" ")[0], #firstname
            i[0].split(" ")[-1], #last_name
            i[0], #fullname
            i[11], #payer_id
            i[1], #player_position
            i[2], # team
            i[3], #age
            i[5], # one_qb_rank_ecr
            i[6],# sf_rank_ecr
            i[7], #ecr_pos
            int(i[8]), #one_qb_value
            int(i[9]), # sf_value
            i[12], # insert_date
        ]
        for i in dp_players
    ]

    return dp_players_preped


@task()
def data_validation(dp_players_preped: list):
    return dp_players_preped if len(dp_players_preped) > 0 else False


@task()
def dp_player_load(dp_players_preped: list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_docker")
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
                fp_player_id,
                player_position,
                team,
                age,
                one_qb_rank_ecr,
                sf_rank_ecr,
                ecr_pos,
                one_qb_value,
                sf_value,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , player_position = EXCLUDED.player_position
            , team = EXCLUDED.team
            , one_qb_rank_ecr = EXCLUDED.one_qb_rank_ecr
            , sf_rank_ecr = EXCLUDED.sf_rank_ecr
            , ecr_pos = EXCLUDED.ecr_pos
            , one_qb_value = EXCLUDED.one_qb_value
            , sf_value = EXCLUDED.sf_value
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(dp_players_preped),
        page_size=1000,
    )

    print(f"{len(dp_players_preped)} ktc players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.dp_player_ranks"


@task()
def surrogate_key_formatting(table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="postgres_docker")
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

