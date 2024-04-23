from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

import os
# Obtener la ruta del directorio actual del DAG
dag_folder = os.path.dirname(os.path.abspath(__file__))
# Retroceder dos carpetas para llegar al directorio padre del directorio padre
grandparent_folder = os.path.dirname(os.path.dirname(dag_folder))
# Cambiar al directorio padre del directorio padre
os.chdir(grandparent_folder)
# Obtener el directorio de trabajo actual del directorio padre del directorio padre
grandparent_current_directory = os.getcwd()

script_path = os.path.join(grandparent_current_directory, 'Prueba2.py')
print(script_path)


def ejecutar_script():
    try:
        # Importa y ejecuta el script Python
        with open(script_path, 'r') as file:
            code = file.read()
        exec(code)
    except Exception as e:
        print("Error al ejecutar el script:", e)

ejecutar_script()

# Llamada a la función para ejecutar el script
ejecutar_script()

default_args = {
    'owner': 'usuario',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'm_dag_ejecut.',
    default_args=default_args,
    description='Un DAG para ejecutar un script Python',
    schedule_interval='@daily',
)

ejecutar_script_task = PythonOperator(
    task_id='ejecutar_script_python',
    python_callable=ejecutar_script,
    dag=dag,
)
