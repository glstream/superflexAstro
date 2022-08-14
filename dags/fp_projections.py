from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.fp import (
    fp_proj_web_scraper,
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
    description="Web Scaper pulling in Fantasy Pros projections to build Superflex Power Rankings",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 8, 7),
    catchup=False,
    tags=["scraper", "database"],
)
def fp_projections_load():

    player_data = fp_proj_web_scraper()
    player_validation = data_validation(player_data)
    player_load = fp_player_load(player_validation)
    surrogate_key_formatting(player_load)


fp_projections_dag = fp_projections_load()

