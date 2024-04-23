from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

grandparent_current_directory = os.getcwd()
script_path = os.path.join(grandparent_current_directory, 'script.py')

def say_hello():
    print("Hola Mundo")

default_args = {
    'owner': 'usuario',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'm_dag_ejecutaapt',
    default_args=default_args,
    description='Un DAG para ejecutar un script Python',
    schedule_interval='@daily',
)

ejecutar_script_task = PythonOperator(
    task_id='ejecutar_script_',
    python_callable=say_hello,
    dag=dag,
)
