from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

def refresh_cdm():
    conn_id = 'PG_CONNECTION'  
    hook = PostgresHook(conn_id)

    refresh_dm_courier_ledger = """
    REFRESH MATERIALIZED VIEW cdm.dm_courier_ledger
    """

    with hook.get_conn() as connection, connection.cursor() as cursor:
        cursor.execute(refresh_dm_courier_ledger)

default_args = {
    'start_date': datetime(2023, 9, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'refresh_view',
    default_args=default_args,
    description='REFRESH VIEW SUCCESS',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

load_data_task = PythonOperator(
    task_id='refresh_view',
    python_callable=refresh_cdm,
    dag=dag,
)
