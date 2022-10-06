from datetime import datetime, timedelta
from airflow.decorators import dag
from datetime import datetime
from tasks.fc import (
    sf_api_calls,
    sf_data_validation,
    sf_fc_player_load,
    one_qb_api_calls,
    one_qb_data_validation,
    one_qb_fc_player_load,
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
     description="API Calls and player load for fantasy Calc",
     schedule_interval="30 * * * *",
     start_date=datetime(2022,10,06),
     catchup=False,
     tags=["api_call", "database"],
     )

def fc_players_pull():

    sf_player_data = sf_api_calls()
    sf_player_validation = sf_data_validation(sf_player_data)
    sf_fc_player_load(sf_player_validation)
    one_qb_player_data = one_qb_api_calls()
    one_qb_data_validation = one_qb_data_validation(one_qb_player_data)
    one_qb_fc_player_load = one_qb_fc_player_load(one_qb_data_validation)
    surrogate_key_formatting(one_qb_fc_player_load)
    
fc_players_pull = fc_players_pull()