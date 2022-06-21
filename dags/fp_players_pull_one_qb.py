from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.fp_one_qb import (
    fp_web_scraper,
    data_validation,
    fp_player_load,
    surrogate_key_formatting,
)

dag_owner = "dynasty_superflex_db"


@dag(
    default_args={
        "owner": dag_owner,
        "depends_on_past": False,
        "email": ["grayson.stream@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Web Scaper pulling in Fantasy Pros Player data to build Superflex Power Rankings",
    schedule_interval="32 * * * *",
    start_date=datetime(2022, 6, 17),
    catchup=False,
    tags=["scraper", "database"],
)
def fp_players_pull_one_qb():
    player_data = fp_web_scraper()
    player_validation = data_validation(player_data)
    player_load = fp_player_load(player_validation)
    surrogate_key_formatting(player_load)


fp_player_dag_one_qb = fp_players_pull_one_qb()
