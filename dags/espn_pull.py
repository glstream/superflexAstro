from datetime import datetime, timedelta
import requests, json, os
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow import DAG
from psycopg2.extras import execute_batch
from datetime import datetime

dag_owner = "dynasty_superflex_db"

with DAG(
    "espn_pull",
    default_args={
        "owner": dag_owner,
        "depends_on_past": False,
        "email": ["grayson.stream@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Pulling Data from ESPN API for Player Projections",
    schedule_interval="@hourly",
    start_date=datetime(2022, 6, 8),
    catchup=False,
    tags=["requests", "api", "database"],
) as dag:

    def espn_projections_pull() -> None:
        filters = {
            "players": {
                "filterStatsForExternalIds": {"value": [2021, 2022]},
                "filterSlotIds": {
                    "value": [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                        23,
                        24,
                    ]
                },
                "filterStatsForSourceIds": {"value": [0, 1]},
                "sortAppliedStatTotal": {
                    "sortAsc": False,
                    "sortPriority": 3,
                    "value": "102022",
                },
                "sortDraftRanks": {"sortPriority": 2, "sortAsc": True, "value": "PPR"},
                "sortPercOwned": {"sortPriority": 4, "sortAsc": False},
                "limit": 1075,
                "offset": 0,
                "filterRanksForScoringPeriodIds": {"value": [1]},
                "filterRanksForRankTypes": {"value": ["PPR"]},
                "filterRanksForSlotIds": {"value": [0, 2, 4, 6, 17, 16]},
                "filterStatsForTopScoringPeriodIds": {
                    "value": 2,
                    "additionalValue": ["002022", "102022", "002021", "022022"],
                },
            }
        }

        headers = {"x-fantasy-filter": json.dumps(filters)}

        season = str(datetime.now().year)
        url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{season}/segments/0/leaguedefaults/3?scoringPeriodId=0&view=kona_player_info"
        req = requests.get(url, headers=headers)
        res = req.json()

        top_dir = os.getcwd()
        # data
        # CONFIGS
        data_path = os.path.join(top_dir, "espn_projections.txt")
        print(data_path)

        with open(data_path, "w") as file:
            file.write(json.dumps(res["players"]))

        return "API RUN COMPLETE"

    def data_validation():
        top_dir = os.getcwd()
        data_path = os.path.join(top_dir, "espn_projections.txt")
        print(data_path)
        with open(data_path, "r") as player_data:
            players = json.load(player_data)
        return True if len(players) > 0 else False

    def projections_load():

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()

        cursor = conn.cursor()
        print("Connection established")

        top_dir = os.getcwd()

        # CONFIGS
        data_path = os.path.join(top_dir, "espn_projections.txt")
        print(data_path)

        with open(data_path, "r") as player_data:
            players = json.load(player_data)

        enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        print("ENTRY TIME", enrty_time)

        espn_players = []
        for player in players:
            espn_players.append(
                [
                    player["player"]["fullName"].split(" ")[0],
                    player["player"]["fullName"].replace("'", "").replace('"', "").replace(' III', "").replace(' II', "").replace(' Jr.', "").split(" ")[-1],
                    player["player"]["fullName"],
                    player["id"],
                    player["player"]["draftRanksByRankType"]["PPR"].get("rank", -1),
                    player["player"]["draftRanksByRankType"]["PPR"].get(
                        "auctionValue", -1
                    ),
                    round(player["player"]["stats"][-1].get("appliedTotal", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("53", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("42", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("43", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("23", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("24", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("25", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("0", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("1", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("3", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("4", 0)),
                    round(player["player"]["stats"][-1]["stats"].get("20", 0)),
                    enrty_time,
                ]
            )

        execute_batch(
            cursor,
            """INSERT INTO dynastr.espn_player_projections (
            player_first_name,
            player_last_name,
            player_full_name,
            espn_player_id,
            ppr_rank,
            ppr_auction_value,
            total_projection,
            recs,
            rec_yards,
            rec_tds,
            carries,
            rush_yards,
            rush_tds,
            pass_attempts,
            pass_completions,
            pass_yards,
            pass_tds,
            pass_ints,
            insert_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (espn_player_id) DO UPDATE
        SET ppr_rank=excluded.ppr_rank,
        ppr_auction_value=excluded.ppr_auction_value,
        total_projection=excluded.total_projection,
        recs=excluded.recs,
        rec_yards=excluded.rec_yards,
        rec_tds=excluded.rec_tds,
        carries=excluded.carries,
        rush_yards=excluded.rush_yards,
        rush_tds=excluded.rush_tds,
        pass_attempts=excluded.pass_attempts,
        pass_completions=excluded.pass_completions,
        pass_yards=excluded.pass_yards,
        pass_tds=excluded.pass_tds,
        pass_ints=excluded.pass_ints,
        insert_date=excluded.insert_date
            """,
            tuple(espn_players),
            page_size=1000,
        )

        conn.commit()
        cursor.close()

        print(f"{len(espn_players)} espn players to inserted or updated.")

        return "LOAD COMPLETE"

    api_pull = PythonOperator(
        task_id="get_raw_projections",
        provide_context=True,
        python_callable=espn_projections_pull,
    )

    data_validation = PythonOperator(
        task_id="projections_validation",
        provide_context=True,
        python_callable=data_validation,
    )

    projections_load = PythonOperator(
        task_id="load_projections",
        provide_context=True,
        python_callable=projections_load,
    )

    surrogate_key_formatting = PostgresOperator(
        task_id="surrogate_key_formatting",
        postgres_conn_id="postgres_default",
        sql="sql/espn_key_formatting.sql",
    )


api_pull >> data_validation >> projections_load >> surrogate_key_formatting
