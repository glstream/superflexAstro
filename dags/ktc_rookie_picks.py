from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.ktc_rookies import (
    ktc_one_qb_rookies,
    one_qb_data_validation,
    ktc_one_qb_rookies_load
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
    description="Selenium web scrapper for ktc rookie picks",
    schedule_interval="@daily",
    start_date=datetime(2023, 3, 12),
    catchup=False,
    tags=["scraper", "database"],
)
def ktc_rookie_picks():

    player_data = ktc_one_qb_rookies()
    player_validation = one_qb_data_validation(player_data)
    player_load = ktc_one_qb_rookies_load(player_validation)


ktc_rookie_picks = ktc_rookie_picks()