import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
#API
KEY = os.getenv("KEY")

#SQL
dbname= os.getenv("dbname")
user= os.getenv("user")
clave= os.getenv("clave")
host= os.getenv("host")
port2= os.getenv("port2")

