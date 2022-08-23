from datetime import datetime, timedelta
from airflow.decorators import dag
from tasks.meta import get_user_meta, add_geo_meta, geo_transforms, history_meta_load

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
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 7),
    catchup=False,
    tags=["user_metadata"],
)
def user_meta_load():

    user_metadata = get_user_meta()
    user_geo_metadata = add_geo_meta(user_metadata)
    preped_meta = geo_transforms(user_geo_metadata)
    history_meta_load(preped_meta)


user_meta_load_dag = user_meta_load()

