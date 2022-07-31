from datetime import datetime, timedelta
from airflow.decorators import dag
from datetime import datetime
from tasks.dp import (
    dp_web_scrapper,
    data_validation,
    dp_player_load,
    surrogate_key_formatting,
)

dag_owner = "grayson.stream"

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
     description="Web Scrapper for Dynastry Process Values",
     schedule_interval="0 14 * * *",
     start_date=datetime(2022,7,29),
     catchup=False,
     tags=["scraper", "database"],
     )

def dp_players_pull():

    player_data = dp_web_scrapper()
    player_validation = data_validation(player_data)
    player_load = dp_player_load(player_validation)
    surrogate_key_formatting(player_load)
    
dp_players_pull = dp_players_pull()