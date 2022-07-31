from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.ktc import (
    ktc_web_scraper,
    data_validation,
    ktc_player_load,
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
    description="Web Scaper pulling in KTC Player data to build Superflex Power Rankings",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 6, 8),
    catchup=False,
    tags=["scraper", "database"],
)
def ktc_player_task_api():

    player_data = ktc_web_scraper()
    player_validation = data_validation(player_data)
    player_load = ktc_player_load(player_validation)
    surrogate_key_formatting(player_load)


ktc_player_dag = ktc_player_task_api()

