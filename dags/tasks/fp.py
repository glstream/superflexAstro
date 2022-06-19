from airflow.decorators import task
import json, requests, re
from psycopg2.extras import execute_batch
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresHook


@task
def fp_web_scraper():
    fp_base = "https://www.fantasypros.com/nfl/rankings/dynasty-overall.php"
    res = requests.get(fp_base)

    soup = BeautifulSoup(res.text, "html.parser")
    scripts = soup.find_all("script")

    fp_script_array = scripts[47]

    # returning the string of the script tag for parsing
    players_script = fp_script_array.contents[0]

    START_REGEX = "var ecrData ="
    start_substr_num = players_script.find(START_REGEX) + len(START_REGEX) + 1

    # second occurance of semicolin
    end_regex = ";"
    fourth_semicolin_num = [m.start() for m in re.finditer(r";", players_script)][3]

    players_array = players_script[start_substr_num:fourth_semicolin_num]
    fp_players_json = json.loads(players_array)

    return fp_players_json


@task
def data_validation(fp_players_json: dict):
    return fp_players_json if len(fp_players_json) > 0 else False


@task
def fp_player_load(fp_players_json):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    fp_players = []

    for fp_player in fp_players_json["players"]:
        fp_players.append(
            [
                fp_player["player_name"],
                fp_player["player_id"],
                fp_player["player_team_id"],
                fp_player["player_position_id"],
                fp_player["player_positions"],
                fp_player["player_short_name"],
                fp_player["player_eligibility"],
                fp_player["player_yahoo_positions"],
                fp_player["player_page_url"],
                fp_player["player_square_image_url"],
                fp_player["player_image_url"],
                fp_player["player_yahoo_id"],
                fp_player["cbs_player_id"],
                fp_player["player_bye_week"],
                fp_player["player_age"],
                fp_player["player_ecr_delta"],
                fp_player["rank_ecr"],
                fp_player["rank_min"],
                fp_player["rank_max"],
                fp_player["rank_ave"],
                fp_player["rank_std"],
                fp_player["pos_rank"],
                fp_player["tier"],
                enrty_time,
            ]
        )
    execute_batch(
        cursor,
        """INSERT INTO dynastr.fp_player_ranks (
                                            player_name,
                                                    fp_player_id,
                                                    player_team_id,
                                                    player_position_id,
                                                    player_positions,
                                                    player_short_name,
                                                    player_eligibility,
                                                    player_yahoo_positions,
                                                    player_page_url,
                                                    player_square_image_url,
                                                    player_image_url,
                                                    player_yahoo_id,
                                                    cbs_player_id,
                                                    player_bye_week,
                                                    player_age,
                                                    player_ecr_delta,
                                                    rank_ecr,
                                                    rank_min,
                                                    rank_max,
                                                    rank_ave,
                                                    rank_std,
                                                    pos_rank,
                                                    tier,
                                                    insert_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (fp_player_id)
                    DO UPDATE SET rank_ecr = EXCLUDED.rank_ecr
                    , rank_min = EXCLUDED.rank_min
                    , rank_max = EXCLUDED.rank_max
                    , rank_ave = EXCLUDED.rank_ave
                    , rank_std = EXCLUDED.rank_std
                    , pos_rank = EXCLUDED.pos_rank
                    , tier = EXCLUDED.tier;
                    """,
        tuple(fp_players),
        page_size=1000,
    )
    conn.commit()
    cursor.close()

    return "dynastr.fp_player_ranks"


@task()
def surrogate_key_formatting(table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    cursor.execute(
        f"""UPDATE {table_name} 
                        SET player_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(player_name,'.',''), ' Jr', ''), ' III',''),'Jeffery','Jeff'), 'Joshua','Josh'),'William','Will'), ' II', ''),'''',''),'Kenneth','Ken'),'Mitchell','Mitch'),'DWayne','Dee')
                        """
    )
    conn.commit()
    cursor.close()
