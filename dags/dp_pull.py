from datetime import datetime, timedelta
from airflow.decorators import dag
from datetime import datetime
from tasks.dp import *

dag_owner = "grayson.stream"

default_args = {
    "owner": dag_owner,
    "depends_on_past":False,
    "retries":1,
}


@dag(default_args=default_args,
     description="Web Scrapper for Dynastry Process Values",
     schedule_interval="0 14 * * *",
     start_date=datetime(2022,7,29)
     catchup=False,
     tags=["scraper", "database"],
     )
def dp_player_load():
    player_data = dp_web_scrapper()
    player_validation = data_validation(player_data)
    player_load = dp_player_load(player_validation)
    
    surrogate_key_formatting(player_load)
    

dp_player_load = dp_player_load()