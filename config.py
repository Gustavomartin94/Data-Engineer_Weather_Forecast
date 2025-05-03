import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Retrieve environment variables
# API
KEY = os.getenv("KEY")

# SQL
dbname = os.getenv("dbname")
user = os.getenv("user")
clave = os.getenv("clave")
host = os.getenv("host")
port2 = os.getenv("port2")
