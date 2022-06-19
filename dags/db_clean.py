
from airflow.operators.python_operator import  PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator,PostgresHook
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta

dag_owner = 'dynasty_superflex_db'
dynasty_sf_config = Variable.get(dag_owner, deserialize_json=True)

with DAG(
    'db_clean',
    default_args={
        'owner': dag_owner,
        'depends_on_past': False,
        'email': ['grayson.stream@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        },
    description='Cleaning user tables to maintain page load times',
    schedule_interval="@daily",
    start_date=datetime(2022, 6, 9),
    catchup=False,
    tags=['database'],

) as dag:

    def row_check(table_name):
        print(table_name)
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"""select count(*) from dynastr.{table_name};""")
        db_row_return = cursor.fetchall()
        print(f"select count(*) from dynastr.{table_name};")
        cursor.close()
        conn.close()
        row_count = db_row_return[0][0]
        return True if row_count > 5000 else False

    for table in dynasty_sf_config['tables']:

        row_count_check = ShortCircuitOperator(
            task_id=f"row_count_check_{table}",
            provide_context = True,
            python_callable=row_check,
            op_args=[table]
        )


        clean_dbs = PostgresOperator(
            task_id=f"current_{table}_clean_task",
            postgres_conn_id="postgres_default",
            sql=f"DELETE FROM dynastr.{table};",
        )
        row_count_check >> clean_dbs