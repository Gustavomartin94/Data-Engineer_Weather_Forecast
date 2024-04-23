from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


from pathlib import Path
# Construct the file path using pathlib
file_path = Path('C:/Users/gusta/Desktop/Data Engineer/Scraping/Scraping Dolar.py')


import docker

def ejecutar_script():
    try:
        # Verificar si la imagen ya existe
        client = docker.from_env()
        image_tag = 'mi_imagen'
        if image_tag not in [image.tags[0] for image in client.images.list()]:
            # Si la imagen no existe, construirla
            client.images.build(path=file_path, dockerfile='Dockerfile', tag=image_tag)
        
        # Ejecutar un contenedor basado en la imagen
        client.containers.run(image_tag, detach=True)
        
        print("Imagen Docker construida y contenedor ejecutado exitosamente.")
    except Exception as e:
        print("Error al ejecutar el script:", e)

default_args = {
    'owner': 'usuario',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'm_dag_ejecutar',
    default_args=default_args,
    description='Un DAG para ejecutar un script Python',
    schedule_interval='@daily',
)

ejecutar_script_task = PythonOperator(
    task_id='ejecutar_script_python',
    python_callable=ejecutar_script,
    dag=dag,
)
