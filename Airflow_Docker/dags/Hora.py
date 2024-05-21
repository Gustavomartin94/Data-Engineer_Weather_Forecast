from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_current_time():
    now = datetime.now()
    print(f"La hora actual es: {now}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'print_current_time_dag',
    default_args=default_args,
    description='Un DAG que imprime la hora actual',
    schedule_interval=timedelta(minutes=10),  # Ejecuta cada 10 minutos
)

print_time_task = PythonOperator(
    task_id='print_current_time_task',
    python_callable=print_current_time,
    dag=dag,
)

print_time_task
