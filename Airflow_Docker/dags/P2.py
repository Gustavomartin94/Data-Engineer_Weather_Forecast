from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
from config import KEY
from constants import cities_coordinates

def say_hello():
    print(KEY)

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'usuario',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'hola_mundo2',
    default_args=default_args,
    description='Un simple DAG que imprime "Hola Mundo"',
    schedule_interval=None,  # Deshabilitar la planificación automática
)

# Definir la tarea que ejecuta la función say_hello
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=say_hello,
    dag=dag,
)

