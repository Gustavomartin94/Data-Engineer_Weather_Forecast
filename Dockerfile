# Usar la imagen oficial de Python
FROM python:3.11

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar el archivo requirements.txt al contenedor
COPY Entregable_Gustavo_Martin.ipynb .

# Copiar el archivo requirements.txt al contenedor
COPY requirements.txt .

# Instalar las dependencias del proyecto
RUN pip install -r requirements.txt

# CMD para ejecutar el script Python (-u permite ver la salida en tiempo real, sin necesida que se almacene en buffer)
CMD ["python", "-u", "Entregable_Gustavo_Martin.ipynb"]
