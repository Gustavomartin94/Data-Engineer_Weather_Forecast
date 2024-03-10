Este trabajo consiste en obtener una API  del pronóstico del clima de los próximos 2 días, cada 6 horas (Temperatura, precipitaciones y velocidad del viento) y exportarlo en una base de datos de Redshift. Para lo cual se realizaron los siguientes pasos

1- Importación de bibliotecas y definición de variables: El código comienza importando las bibliotecas necesarias, como datetime, meteomatics.api, pandas, y otras, además de importar variables y constantes desde archivos de configuración y definir coordenadas de ciudades y parámetros climáticos.
Se deben instalar estas bibliotecas meteomatics, python-dotenv, sqlalchemy y psycopg2

2- Consulta de datos meteorológicos: Se establece un rango de fechas para la consulta de datos meteorológicos de las próximas 48 horas utilizando la API de Meteomatics. Se iteran sobre las coordenadas de las ciudades para obtener datos meteorológicos como temperatura, precipitación y velocidad del viento para cada ciudad.

3- Creación de DataFrame: Los datos obtenidos se almacenan en DataFrames individuales, uno para cada ciudad, y luego se concatenan en un solo DataFrame llamado df_final.

4- Procesamiento de datos: Se agregan columnas adicionales al DataFrame, como la fecha de actualización y se reordenan las columnas según el formato deseado.

5- Conexión a la base de datos: Se establece una conexión con una base de datos PostgreSQL utilizando los detalles de conexión proporcionados en el archivo de configuración.

6- Inserción de datos: Se prepara una consulta SQL para insertar los datos del DataFrame en la tabla de la base de datos. Luego, se itera sobre las filas del DataFrame y se ejecuta la consulta SQL para insertar los datos en la base de datos.

7- Confirmación y cierre de la conexión: Se confirman los cambios realizados en la base de datos y se cierra la conexión y el cursor. Esto asegura que los datos se guarden correctamente en la base de datos y se liberen los recursos adecuadamente.