from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
from datetime import datetime
from config import KEY
from constants import cities_coordinates


import psycopg2
from config import dbname, user, clave, host, port2

# Importa la función 'mifuncion' desde 'script.py'
from Script import ETL

# Define los argumentos por defecto del DAG
default_args = {
    'owner': 'tu_nombre',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define el DAG
dag = DAG(
    'nombre_del_dag',
    default_args=default_args,
    description='Descripción del DAG',
    schedule_interval='@daily',  # El DAG se ejecutará diariamente
    start_date=datetime(2024, 4, 22),
    catchup=False,
)

# Define la tarea que utiliza 'mifuncion'
tarea_mi_funcion = PythonOperator(
    task_id='tarea_mi_funcion',
    python_callable=ETL,  # Esta es la función que se ejecutará
    dag=dag,
)

# Aquí podrías definir otras tareas y dependencias si es necesario.
# Por ejemplo, podrías añadir más tareas y establecer dependencias entre ellas.

# Ejemplo de cómo definir dependencias:
# otra_tarea = PythonOperator(
#     task_id='otra_tarea',
#     python_callable=otra_funcion,
#     dag=dag,
# )
# otra_tarea.set_upstream(tarea_mi_funcion)
