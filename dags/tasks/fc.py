import requests
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime

enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")


@task()
def sf_api_calls():
    fc_sf_players = []
    fc_sf_api_calls = {
        "dynasty": "https://api.fantasycalc.com/values/current?isDynasty=true&numQbs=2",
        "redraft": "https://api.fantasycalc.com/values/current?isDynasty=false&numQbs=2",
    }

    for rank_type, api_call in fc_sf_api_calls.items():
        fc_sf_req = requests.get(api_call)
        for i in fc_sf_req.json():
            p = (
                i["player"]["name"]
                .replace("'", "")
                .replace("'", "")
                .replace(" III", "")
                .replace(" II", "")
                .replace(" Jr.", "")
                .split(" ")
            )

            player_record = [
                p[0],
                p[-1],
                i["player"]["name"],
                i["player"]["id"],
                i["player"]["mflId"],
                i["player"]["sleeperId"],
                i["player"]["position"],
                rank_type,
                None,
                None,
                None,
                None,
                i["overallRank"],
                i["positionRank"],
                i["value"],
                i["trend30Day"],
                enrty_time,
            ]
            fc_sf_players.append(player_record)

    return fc_sf_players


@task()
def sf_data_validation(fc_sf_players: list):
    return fc_sf_players if len(fc_sf_players) > 0 else False


@task()
def sf_fc_player_load(fc_sf_players: list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.fc_player_ranks (
                player_first_name,
                player_last_name,
                player_full_name,
                fc_player_id,
                mfl_player_id,
                sleeper_player_id,
                player_position,
                rank_type,
                one_qb_overall_rank,
                one_qb_position_rank,
                one_qb_value,
                one_qb_trend_30_day,
                sf_overall_rank,
                sf_position_rank,
                sf_value,
                sf_trend_30_day,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s)
        ON CONFLICT (player_full_name, rank_type)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , player_position = EXCLUDED.player_position
            , sf_overall_rank = EXCLUDED.sf_overall_rank
            , sf_position_rank = EXCLUDED.sf_position_rank
            , sf_value = EXCLUDED.sf_value
            , sf_trend_30_day = EXCLUDED.sf_trend_30_day
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(fc_sf_players),
        page_size=1000,
    )

    print(f"{len(fc_sf_players)} ktc players to inserted or updated.")
    conn.commit()
    cursor.close()
    return 'done'


@task()
def one_qb_api_calls(sf_player_dependency:str):
    fc_one_qb_players = []
    fc_one_qb_api_calls = {
        "dynasty": "https://api.fantasycalc.com/values/current?isDynasty=true&numQbs=1",
        "redraft": "https://api.fantasycalc.com/values/current?isDynasty=false&numQbs=1",
    }

    for rank_type, api_call in fc_one_qb_api_calls.items():
        fc_one_qb_req = requests.get(api_call)
        for i in fc_one_qb_req.json():
            p = (
                i["player"]["name"]
                .replace("'", "")
                .replace("'", "")
                .replace(" III", "")
                .replace(" II", "")
                .replace(" Jr.", "")
                .split(" ")
            )

            player_record = [
                p[0],
                p[-1],
                i["player"]["name"],
                i["player"]["id"],
                i["player"]["mflId"],
                i["player"]["sleeperId"],
                i["player"]["position"],
                rank_type,
                i["overallRank"],
                i["positionRank"],
                i["value"],
                i["trend30Day"],
                None,
                None,
                None,
                None,
                enrty_time,
            ]
            fc_one_qb_players.append(player_record)
    return fc_one_qb_players


@task()
def one_qb_data_validation(fc_one_qb_players: list):
    return fc_one_qb_players if len(fc_one_qb_players) > 0 else False


@task()
def one_qb_fc_player_load(fc_one_qb_players: list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.fc_player_ranks (
                player_first_name,
                player_last_name,
                player_full_name,
                fc_player_id,
                mfl_player_id,
                sleeper_player_id,
                player_position,
                rank_type,
                one_qb_overall_rank,
                one_qb_position_rank,
                one_qb_value,
                one_qb_trend_30_day,
                sf_overall_rank,
                sf_position_rank,
                sf_value,
                sf_trend_30_day,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s)
        ON CONFLICT (player_full_name, rank_type)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , player_position = EXCLUDED.player_position
            , one_qb_overall_rank = EXCLUDED.one_qb_overall_rank
            , one_qb_position_rank = EXCLUDED.one_qb_position_rank
            , one_qb_value = EXCLUDED.one_qb_value
            , one_qb_trend_30_day = EXCLUDED.one_qb_trend_30_day
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(fc_one_qb_players),
        page_size=1000,
    )

    print(f"{len(fc_one_qb_players)} ktc players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.fc_player_ranks"


@task()
def surrogate_key_formatting(table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    print("Connection established")
    cursor.execute(
        f"""UPDATE {table_name} 
                        SET player_first_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(player_first_name,'.',''), ' Jr', ''), ' III',''),'Jeffery','Jeff'), 'Joshua','Josh'),'William','Will'), ' II', ''),'''',''),'Kenneth','Ken'),'Mitchell','Mitch'),'DWayne','Dee'),'Gabe','Gabriel')
                        """
    )
    conn.commit()
    cursor.close()
    return

