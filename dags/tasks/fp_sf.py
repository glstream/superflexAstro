from airflow.decorators import task
import json, requests, re
from psycopg2.extras import execute_batch
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresHook


@task
def sf_fp_web_scraper():
    fp_base = "https://www.fantasypros.com/nfl/rankings/dynasty-superflex.php"
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

    return sf_fp_players_json


@task
def sf_data_validation(sf_fp_players_json: dict):
    return sf_fp_players_json if len(sf_fp_players_json) > 0 else False


@task
def sf_fp_player_load(sf_fp_players_json):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    sf_fp_players = []

    for sf_fp_player in sf_fp_players_json["players"]:
        sf_fp_players.append(
            [
                sf_fp_player["player_name"],
                sf_fp_player["player_id"],
                sf_fp_player["player_team_id"],
                sf_fp_player["player_position_id"],
                sf_fp_player["player_positions"],
                sf_fp_player["player_short_name"],
                sf_fp_player["player_eligibility"],
                sf_fp_player["player_yahoo_positions"],
                sf_fp_player["player_page_url"],
                sf_fp_player["player_square_image_url"],
                sf_fp_player["player_image_url"],
                sf_fp_player["player_yahoo_id"],
                sf_fp_player["cbs_player_id"],
                sf_fp_player["player_bye_week"],
                sf_fp_player["player_age"],
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                sf_fp_player["player_ecr_delta"],
                sf_fp_player["rank_ecr"],
                sf_fp_player["rank_min"],
                sf_fp_player["rank_max"],
                sf_fp_player["rank_ave"],
                sf_fp_player["rank_std"],
                sf_fp_player["pos_rank"],
                sf_fp_player["tier"],
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
                                                    one_player_ecr_delta,
                                                    one_rank_ecr,
                                                    one_rank_min,
                                                    one_rank_max,
                                                    one_rank_ave,
                                                    one_rank_std,
                                                    one_pos_rank,
                                                    one_tier,
                                                    sf_player_ecr_delta
                                                    sf_rank_ecr,
                                                    sf_rank_min,
                                                    sf_rank_max,
                                                    sf_rank_ave,
                                                    sf_rank_std,
                                                    sf_pos_rank,
                                                    sf_tier,
                                                    insert_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (fp_player_id)
                    DO UPDATE SET sf_rank_ecr = EXCLUDED.sf_rank_ecr
                    , sf_rank_min = EXCLUDED.sf_rank_min
                    , sf_rank_max = EXCLUDED.sf_rank_max
                    , sf_rank_ave = EXCLUDED.sf_rank_ave
                    , sf_rank_std = EXCLUDED.sf_rank_std
                    , sf_pos_rank = EXCLUDED.sf_pos_rank
                    , sf_tier = EXCLUDED.sf_tier;
                    """,
        tuple(sf_fp_players),
        page_size=1000,
    )
    conn.commit()
    cursor.close()

    return "dynastr.fp_player_ranks"


@task()
def sf_surrogate_key_formatting(table_name: str):
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
