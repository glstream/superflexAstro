from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.nfl import (
    nfl_web_scrapper,
    data_validation,
    nfl_player_load,
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
    description="Web Scaper pulling in NFL Player projections to build Superflex Power Rankings",
    schedule_interval="30 */4 * * *",
    start_date=datetime(2022, 8, 7),
    catchup=False,
    tags=["scraper", "database"],
)
def nfl_projections_load():

    player_data = nfl_web_scrapper()
    player_validation = data_validation(player_data)
    player_load = nfl_player_load(player_validation)
    surrogate_key_formatting(player_load)


nfl_projections_dag = nfl_projections_load()

