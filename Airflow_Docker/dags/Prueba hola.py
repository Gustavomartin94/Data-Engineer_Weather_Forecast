
''''
# Obtener la ruta del directorio actual del DAG
dag_folder = os.path.dirname(os.path.abspath(__file__))
# Retroceder dos carpetas para llegar al directorio padre del directorio padre
grandparent_folder = os.path.dirname(os.path.dirname(dag_folder))
# Cambiar al directorio padre del directorio padre
os.chdir(grandparent_folder)
# Obtener el directorio de trabajo actual del directorio padre del directorio padre
grandparent_current_directory = os.getcwd()

script_path = os.path.join(grandparent_current_directory, 'Prueba2.py')

def ejecutar_script():
    # Importa y ejecuta el script Python
    exec(open(script_path).read())
'''
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

def ejecutar_script():
    try:
        # Importa y ejecuta el script Python
        with open(script_path, 'r') as file:
            code = file.read()
        exec(code)
    except Exception as e:
        print("Error al ejecutar el script:", e)


# Llamada a la función para ejecutar el script
ejecutar_script()
