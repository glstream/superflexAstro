from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.fp_sf import (
    sf_fp_web_scraper,
    sf_data_validation,
    sf_fp_player_load,
    sf_surrogate_key_formatting,
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
    description="Web Scaper pulling in sf player values Fantasy Pros Player data to build Superflex Power Rankings",
    schedule_interval="32 * * * *",
    start_date=datetime(2022, 6, 17),
    catchup=False,
    tags=["scraper", "database"],
)
def sf_fp_players_pull():
    player_data = sf_fp_web_scraper()
    player_validation = sf_data_validation(player_data)
    player_load = sf_fp_player_load(player_validation)
    sf_surrogate_key_formatting(player_load)


sf_fp_player_dag = sf_fp_players_pull()
