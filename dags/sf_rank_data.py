from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.sf import (
    fetch_run_data
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
    description="Superflex rankings to harmonize and nromalize data from different source",
    schedule_interval="15 * * * *",
    start_date=datetime(2023, 3,22),
    catchup=False,
    tags=["database"],
)
def sf_rank_data():

    fetch_run_data()


sf_rank_data_dag = sf_rank_data()

