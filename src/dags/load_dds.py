from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

def load_data_to_dds():
    conn_id = 'PG_CONNECTION'
    hook = PostgresHook(conn_id)

    insert_calendar_query = """
    INSERT INTO dds.calendar ("timestamp", "date", "year", "month", "day")
    SELECT DISTINCT 
        order_ts::timestamp AS "timestamp",
        order_ts::date AS "date",
        EXTRACT(YEAR FROM order_ts::timestamp)::smallint AS "year",
        EXTRACT(MONTH FROM order_ts::timestamp)::smallint AS "month",
        EXTRACT(DAY FROM order_ts::timestamp)::smallint AS "day"
    FROM stg.deliveries
    """

    insert_couriers_query = """
    INSERT INTO dds.couriers (id, courier_name)
    SELECT DISTINCT id AS id, name
    FROM stg.couriers
    """

    insert_restaurants_query = """
    INSERT INTO dds.restaurants (id, restaurant_name)
    SELECT DISTINCT id AS id, name
    FROM stg.restaurants
    """

    insert_deliveries_query = """
    INSERT INTO dds.deliveries (order_id, calendar_id, courier_id, rate, "sum", tip_sum)
    SELECT DISTINCT 
        order_id,
        c.id AS calendar_id,
        courier_id,
        rate::smallint,
        "sum"::int,
        tip_sum::int
    FROM stg.deliveries d
    INNER JOIN dds.calendar c ON d.order_ts::timestamp = c."timestamp"
    """

    with hook.get_conn() as connection, connection.cursor() as cursor:
        cursor.execute(insert_calendar_query)
        cursor.execute(insert_couriers_query)
        cursor.execute(insert_restaurants_query)
        cursor.execute(insert_deliveries_query)

default_args = {
    'start_date': datetime(2023, 9, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'load_staging_to_dds',
    default_args=default_args,
    description='Load data from STG to DDS',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

load_data_task = PythonOperator(
    task_id='load_data_to_dds_task',
    python_callable=load_data_to_dds,
    dag=dag,
)
