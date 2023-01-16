from datetime import datetime, timedelta
import requests, json, os, re
from airflow.operators.python_operator import  PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from psycopg2.extras import execute_batch
from datetime import datetime
from bs4 import BeautifulSoup

dag_owner = 'dynasty_superflex_db'

with DAG(
    'ktc_pull',
    default_args={
        'owner': dag_owner,
        'depends_on_past': False,
        'email': ['grayson.stream@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='Web Scapper pulling in KTC Player data to build Superflex Power Rankings',
   schedule_interval="@hourly",
    start_date=datetime(2022, 6, 8),
    catchup=False,
    tags=['scraper', 'database'],
) as dag:
    def ktc_web_scraper():
        page = "0"
        league_format = {"single_qb": "1", "sf": "2"}

        ktc_base = f"https://keeptradecut.com/dynasty-rankings?page={page}&filters=QB|WR|RB|TE|RDP&format={league_format['sf']}"

        res = requests.get(ktc_base)

        soup = BeautifulSoup(res.text, "html.parser")

        scripts = soup.find_all("script")
        script_var_array = scripts[6]

        # returning the string of the script tag for parsing
        players_script = script_var_array.contents[0]

        START_REGEX = "var playersArray ="

        start_substr_num = players_script.find(START_REGEX) + len(START_REGEX) + 1

        # second occurance of semicolin
        end_regex = ";"
        second_semicolin_num = [m.start() for m in re.finditer(r";", players_script)][1]
        top_dir = os.getcwd()

        players_array = players_script[start_substr_num:second_semicolin_num]
        ktc_players_json = json.loads(players_array)

        kct_data_path = os.path.join(top_dir, "ktc_player_rankings.txt")
        print(kct_data_path)

        with open(kct_data_path, "w") as file:
            file.write(json.dumps(ktc_players_json))

        return "WEB SCRAPER COMPLETE"
    
    def data_validation():
        top_dir = os.getcwd()
        data_path = os.path.join(top_dir, "ktc_player_rankings.txt")
        print(data_path)
        with open(data_path, "r") as player_data:
            players = json.load(player_data)
        return True if len(players) > 0 else False
    
    def ktc_player_load():

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()

        cursor = conn.cursor()
        print("Connection established")

        top_dir = os.getcwd()

        # CONFIGS
        data_path = os.path.join(top_dir, "ktc_player_rankings.txt")

        with open(data_path, "r") as player_data:
            ktc_players_data = json.load(player_data)

        enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        
        ktc_players = []

        for ktc_player in ktc_players_data:
            ktc_players.append(
                [
                    ktc_player["playerName"],
                    ktc_player["playerID"],
                    ktc_player["slug"],
                    ktc_player["position"],
                    ktc_player["positionID"],
                    ktc_player["team"],
                    ktc_player["rookie"],
                    ktc_player["college"],
                    ktc_player["age"],
                    ktc_player["heightFeet"],
                    ktc_player["heightInches"],
                    ktc_player["weight"],
                    ktc_player["seasonsExperience"],
                    ktc_player["pickRound"],
                    ktc_player["pickNum"],
                    ktc_player["oneQBValues"]["value"],
                    ktc_player["oneQBValues"]["startSitValue"],
                    ktc_player["oneQBValues"]["rank"],
                    ktc_player["oneQBValues"]["overallTrend"],
                    ktc_player["oneQBValues"]["positionalTrend"],
                    ktc_player["oneQBValues"]["positionalRank"],
                    ktc_player["oneQBValues"]["rookieRank"],
                    ktc_player["oneQBValues"]["rookiePositionalRank"],
                    ktc_player["oneQBValues"]["kept"],
                    ktc_player["oneQBValues"]["traded"],
                    ktc_player["oneQBValues"]["cut"],
                    ktc_player["oneQBValues"]["overallTier"],
                    ktc_player["oneQBValues"]["positionalTier"],
                    ktc_player["oneQBValues"]["rookieTier"],
                    ktc_player["oneQBValues"]["rookiePositionalTier"],
                    ktc_player["oneQBValues"]["startSitOverallRank"],
                    ktc_player["oneQBValues"]["startSitPositionalRank"],
                    ktc_player["oneQBValues"]["startSitOverallTier"],
                    ktc_player["oneQBValues"]["startSitPositionalTier"],
                    ktc_player["oneQBValues"]["startSitOneQBFlexTier"],
                    ktc_player["oneQBValues"]["startSitSuperflexFlexTier"],
                    ktc_player["superflexValues"]["value"],
                    ktc_player["superflexValues"]["startSitValue"],
                    ktc_player["superflexValues"]["rank"],
                    ktc_player["superflexValues"]["overallTrend"],
                    ktc_player["superflexValues"]["positionalTrend"],
                    ktc_player["superflexValues"]["positionalRank"],
                    ktc_player["superflexValues"]["rookieRank"],
                    ktc_player["superflexValues"]["rookiePositionalRank"],
                    ktc_player["superflexValues"]["kept"],
                    ktc_player["superflexValues"]["traded"],
                    ktc_player["superflexValues"]["cut"],
                    ktc_player["superflexValues"]["overallTier"],
                    ktc_player["superflexValues"]["positionalTier"],
                    ktc_player["superflexValues"]["rookieTier"],
                    ktc_player["superflexValues"]["rookiePositionalTier"],
                    ktc_player["superflexValues"]["startSitOverallRank"],
                    ktc_player["superflexValues"]["startSitPositionalRank"],
                    ktc_player["superflexValues"]["startSitOverallTier"],
                    ktc_player["superflexValues"]["startSitPositionalTier"],
                    enrty_time,
                ]
            )

        execute_batch(cursor, """
            INSERT INTO dynastr.ktc_player_ranks (
                player_name,
                ktc_player_id,
                slug,
                position,
                position_id,
                team,
                rookie,
                college,
                age,
                height_feet,
                height_inches,
                weight,
                season_experience,
                pick_round,
                pink_num,
                one_qb_value,
                start_sit_value,
                rank,
                overall_trend,
                positional_trend,
                positional_rank,
                rookie_rank,
                rookie_positional_rank,
                kept,
                traded,
                cut,
                overall_tier,
                positional_tier,
                rookie_tier,
                rookie_positional_tier,
                start_sit_overall_rank,
                start_sit_positional_rank,
                start_sit_overall_tier,
                start_sit_positional_tier,
                start_sit_oneQB_flex_tier,
                start_sit_superflex_flex_tier,
                sf_value,
                sf_start_sit_value,
                sf_rank,
                sf_overall_trend,
                sf_positional_trend,
                sf_positional_rank,
                sf_rookie_rank,
                sf_rookie_positional_rank,
                sf_kept,
                sf_traded,
                sf_cut,
                sf_overall_tier,
                sf_positional_tier,
                sf_rookie_tier,
                sf_rookie_positional_tier,
                sf_start_sit_overall_rank,
                sf_start_sit_positional_rank,
                sf_start_sit_overall_tier,
                sf_start_sit_positional_tier,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ktc_player_id)
        DO UPDATE SET sf_rank = EXCLUDED.sf_rank
            , one_qb_value = EXCLUDED.one_qb_value
            , sf_value = EXCLUDED.sf_value;
        """, tuple(ktc_players), page_size=1000)  

        print(f"{len(ktc_players)} ktc players to inserted or updated.")        
        conn.commit()
        cursor.close()
    



    ktc_web_scraper = PythonOperator(
        task_id='ktc_web_scraper_source',
        provide_context=True,
        python_callable=ktc_web_scraper
    )

    data_validation = PythonOperator(
        task_id='data_validation',
        provide_context=True,
        python_callable=data_validation
    )

    ktc_player_load = PythonOperator(
        task_id='ktc_player_load',
        provide_context=True,
        python_callable=ktc_player_load
    )
    surrogate_key_formatting = PostgresOperator(
        task_id="surrogate_key_formatting",
        postgres_conn_id="postgres_default",
        sql="sql/ktc_key_formatting.sql",
    )

ktc_web_scraper >> data_validation >> ktc_player_load >> surrogate_key_formatting

