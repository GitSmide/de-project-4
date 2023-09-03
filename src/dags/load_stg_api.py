import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import psycopg2
from psycopg2 import sql
import requests

api_key = '25c27781-8fde-4b30-a22e-524044a7580f'
your_nickname = 'SMIDE'
your_cohort_number = '15'

def call_api(url):
    response = requests.get(url, headers={
        'X-Nickname': your_nickname,
        'X-Cohort': your_cohort_number,
        'X-API-KEY': api_key
    })
    if response.status_code == 200:
        return response.json()
    else:
        return []

def load_data_to_staging():
    limit = 50
    sort_field = 'update_ts'
    sort_direction = 'asc'

    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    offset = 0

    conn_id = 'PG_CONNECTION'  
    hook = PostgresHook(conn_id)

    while True:
        restaurant_url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        restaurant_data = call_api(restaurant_url)
        if not restaurant_data:
            break

        with hook.get_conn() as connection, connection.cursor() as cursor:
            insert_query = sql.SQL("INSERT INTO stg.restaurants (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING")
            for row in restaurant_data:
                cursor.execute(insert_query, (row['_id'], row['name']))

        offset += limit

    offset = 0
    while True:
        courier_url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        courier_data = call_api(courier_url)
        if not courier_data:
            break

        with hook.get_conn() as connection, connection.cursor() as cursor:
            insert_query = sql.SQL("INSERT INTO stg.couriers (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING")
            for row in courier_data:
                cursor.execute(insert_query, (row['_id'], row['name']))

        offset += limit

    offset = 0
    while True:
        formatted_start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
        formatted_end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
        formatted_start_date = formatted_start_date.replace(' ', '%20')
        formatted_end_date = formatted_end_date.replace(' ', '%20')
        delivery_url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id=&from={formatted_start_date}&to={formatted_end_date}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        delivery_data = call_api(delivery_url)
        if not delivery_data:
            break

        with hook.get_conn() as connection, connection.cursor() as cursor:
            insert_query = sql.SQL("INSERT INTO stg.deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, total, tip_sum) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING")
            for row in delivery_data:
                cursor.execute(insert_query, (row['order_id'], row['order_ts'], row['delivery_id'], row['courier_id'], row['address'], row['delivery_ts'], row['rate'], row['sum'], row['tip_sum']))

        offset += limit
    
    connection.close()

default_args = {
    'start_date': datetime(2023, 9, 1), 
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_extraction_and_loading',
    default_args=default_args,
    description='Extract and load data from API to PostgreSQL',
    schedule_interval=timedelta(days=1),  
    catchup=False,
)

extract_and_load_data_task = PythonOperator(
    task_id='extract_and_load_data',
    python_callable=load_data_to_staging,
    dag=dag,
)

extract_and_load_data_task
