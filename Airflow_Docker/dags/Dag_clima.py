from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define la ruta al script Python
script_path = r'C:\Users\gusta\Desktop\Data Engineer\Semana 7 - Dataframes con SQLalchemy y Psycopg\TP Entregable\Script.py'

# Define los argumentos predeterminados del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 6),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False,
}

# Define el DAG
dag = DAG(
    'dag_clima',
    default_args=default_args,
    description='Ejecutar script Python diariamente',
    schedule_interval=timedelta(days=1),  # Ejecutar diariamente
)

# Define el operador BashOperator para ejecutar el script Python
ejecutar_mi_script = BashOperator(
    task_id='ejecutar_mi_script_task',
    bash_command=f'python "{script_path}"',  # Asegúrate de que la ruta esté correctamente encerrada entre comillas dobles
    dag=dag,
)

# Define la secuencia de tareas en el DAG
ejecutar_mi_script

