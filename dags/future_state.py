from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain
from airflow import DAG
from airflow.models import Variable

dag_owner = 'dynasty_superflex_db'
# dynasty_sf_config = Variable.get(dag_owner, deserialize_json=True)

with DAG(
    "future_state_dag",
    default_args={
        'owner': dag_owner,
        'depends_on_past': True,
        'email': ['grayson.stream@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description="""Pipeline that pull all sources for each sport 
                inserts them into the database then runs the analytical 
                loads and transforms the data.""",
    schedule_interval="30 14 * * *",
    start_date=datetime(2022, 6, 8),
    catchup=False,
    tags=['future_state']
) as dag:
    oltp_groups = []
    custer_provision_message = DummyOperator(task_id='custer_provision_message')

    def table_meta_push(table,k, date_, ti):
        print('pushing data')
        if k < 2:
            ti.xcom_push(key='CLUSTER_NEEDED', value={'cluster_size':'LARGE - D8', 'sport':table, 'date':date_} )
        else:
            ti.xcom_push(key='CLUSTER_NEEDED', value={'cluster_size':'SMALL - 3', 'sport':table,'date':date_ })
        return 

    def table_meta_pull(table, ti):
        table_name = ti.xcom_pull(key='CLUSTER_NEEDED', task_ids=[f'oltp_{table}.need_cluster_settings_{table}'])
        print('TABLE NAME', table_name, 'TABLE:',table)
        return table_name


    sport_tables = sorted(['nfl'])

    for k,sport in enumerate(sport_tables):
        tg_id = f'oltp_{sport}'

        with TaskGroup(group_id=tg_id) as tg1:
                extract = DummyOperator(task_id=f'extract_{sport}_source')
                load = DummyOperator(task_id=f'load_{sport}_target')

                validate_ = PythonOperator(
                    task_id = f'need_cluster_settings_{sport}',
                        provide_context = True,
                        python_callable=table_meta_push,
                        op_args=[sport, k, """{{ ds }}"""]
                )
                extract >> load >> validate_ >> custer_provision_message


                oltp_groups.append(tg1)

    olap_groups = []
    for k,sport in enumerate(sport_tables):
        tg_id = f'olap_{sport}'

        with TaskGroup(group_id=tg_id) as tg2:
                load = DummyOperator(task_id=f'analytical_load_{sport}')
                transform = DummyOperator(task_id=f'indexing_{sport}') 
                provison_ = PythonOperator(
                        task_id = f'provision_new_cluster_settings_{sport}',
                         provide_context = True,
                         python_callable=table_meta_pull,
                         op_args=[sport]
                    )                
                provison_ >> load >> transform

                olap_groups.append(tg2)

    start_etl_loads = DummyOperator(
        task_id=f'start_etl_loads'
    )
    etl_loads_complete_message = DummyOperator(
        task_id='etl_loads_complete_message',
        trigger_rule=TriggerRule.NONE_FAILED
    )

chain(start_etl_loads, [task for task in oltp_groups], [p for p in olap_groups], etl_loads_complete_message)
