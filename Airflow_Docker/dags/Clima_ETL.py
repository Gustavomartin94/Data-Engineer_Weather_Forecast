from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import pendulum


# Importar la funciones

from ETL import Extract_clima, Transform_clima, Load_clima


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
    'ETL_Clima',
    default_args=default_args,
    description='Descripción del DAG',
    schedule_interval='@daily',  # El DAG se ejecutará diariamente
    start_date=datetime(2024, 4, 22, tzinfo=pendulum.timezone('America/Argentina/Buenos_Aires')),
    catchup=False,
)

# Tarea de extracción
tarea_extract_clima = PythonOperator(
    task_id='extract_clima',
    python_callable=Extract_clima,  
    dag=dag,
)
# Tarea de transformación
tarea_transform_clima = PythonOperator(
    task_id='transform_clima',
    python_callable=Transform_clima,
    dag=dag,
)

# Tarea de carga
tarea_load_clima = PythonOperator(
    task_id='load_clima',
    python_callable=Load_clima,
    dag=dag,
)

# Tarea de envío de correo electrónico
tarea_send_email = EmailOperator(
    task_id='send_email',
    to='pruebaairflow0@gmail.com',
    subject='ETL Clima Completo',
    html_content='<p>El proceso ETL de clima ha finalizado exitosamente.</p>',
    dag=dag,
)

# Establecer dependencias entre las tareas
tarea_extract_clima >> tarea_transform_clima >> tarea_load_clima >> tarea_send_email
