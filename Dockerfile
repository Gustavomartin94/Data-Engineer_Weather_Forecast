# Usar la imagen oficial de Python
FROM python:3.11

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar todos los archivos y directorios del directorio actual al contenedor
COPY . .

# Instalar las dependencias del proyecto
RUN pip install -r requirements.txt

# Establecer la zona horaria en el contenedor
ENV TZ=America/Argentina/Buenos_Aires

# CMD para ejecutar el script Python (-u permite ver la salida en tiempo real, sin necesida que se almacene en buffer)
CMD ["python", "-u", "Script.py"]