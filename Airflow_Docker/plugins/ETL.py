import requests
import pandas as pd
from datetime import datetime
from config import KEY
from constants import cities_coordinates
import pendulum

import psycopg2
from config import dbname, user, clave, host, port2

def Extract_clima():

    # Definir la URL base para la API de OpenWeatherMap
    base_url = "http://api.openweathermap.org/data/2.5/forecast"

    # Coordenadas de las ubicaciones deseadas
    coordinates = [(city, coord[0], coord[1]) for city, coord in cities_coordinates.items()]

    # Definir los parámetros de la consulta
    parameters = {
        'units': 'metric',  # Para obtener unidades métricas
        'appid': KEY,  # Tu clave de API de OpenWeatherMap
        'cnt': 4  # Número de períodos de pronóstico que deseas recibir
    }

    # Configurar la zona horaria de Argentina
    timezone = pendulum.timezone('America/Argentina/Buenos_Aires')

    # Lista para almacenar los datos de cada ubicación
    data_list = []

    # Realizar la consulta a la API para cada ubicación
    for city, lat, lon in coordinates:
        # Construir la URL para la consulta
        query_params = parameters.copy()
        query_params['lat'] = lat
        query_params['lon'] = lon

        # Realizar la solicitud GET a la API de OpenWeatherMap
        response = requests.get(base_url, params=query_params)
        
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Convertir la respuesta a formato JSON
            weather_data = response.json()

            # Extraer los datos relevantes del pronóstico futuro
            forecast_data = weather_data['list']
            for forecast in forecast_data:
                # Convertir la fecha y hora a la zona horaria de Argentina
                forecast_time = pendulum.parse(forecast['dt_txt']).in_timezone(timezone)
                
                temperature = forecast['main']['temp']
                humidity = forecast['main']['humidity']
                wind_speed = forecast['wind']['speed']
                # Agregar los datos a la lista
                data_list.append({
                    'lat':lat,
                    'lon':lon,
                    'Ciudad': city,
                    'Fecha y Hora': forecast_time,
                    'Temperatura C': temperature,
                    'Humedad Relativa %': humidity,
                    'Velocidad de viento m/s': wind_speed,
                })
        else:
            print(f"Error al obtener datos para {symbol}: {response.status_code}")

    # Crear un DataFrame a partir de los datos recopilados
    df = pd.DataFrame(data_list)

    return df

def Transform_clima():
    
    df=Extract_clima()
    
    # Configurar la zona horaria de Argentina
    timezone = pendulum.timezone('America/Argentina/Buenos_Aires')
    
    # Convertir la cadena de fecha y hora a formato datetime
    df['Fecha y Hora'] = pd.to_datetime(df['Fecha y Hora'])

    # Formatear la columna 'Fecha y Hora' a solo fecha y hora (sin segundos)
    df['Fecha y Hora'] = df['Fecha y Hora'].dt.strftime('%Y-%m-%d %H:%M')

    # Agregar la columna de Fecha_Actualizacion
    fecha_actualizacion = pendulum.now(timezone).strftime('%Y-%m-%d %H:%M:%S')
    df['Fecha_Actualizacion'] = fecha_actualizacion

    # Agregar una columna de clave primaria e indice
    df['Primary_Key'] = df['Ciudad'] + ' ' + df['Fecha y Hora']

    # Reordenar las columnas
    df = df[['lat','lon','Fecha y Hora','Ciudad',  'Temperatura C', 'Humedad Relativa %', 'Velocidad de viento m/s','Fecha_Actualizacion', 'Primary_Key']]

    return df


 # CARGA A REDSHIFT
def Load_clima():

    df_final= Transform_clima()
    
    #cedenciales
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=clave,
        host=host,
        port=port2
    )

    # Crear cursor
    cur = conn.cursor()

    # Insertar datos
    sql = "INSERT INTO clima VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    # Actualiza datos, borrando los anteriores cuando el Primary_key se repite por actualización
    sql_quitar_anteriores = """
        DELETE FROM clima 
        WHERE (Primary_Key, fecha_actualizacion) NOT IN (
            SELECT Primary_Key, fecha_actualizacion 
            FROM (
                SELECT Primary_Key, fecha_actualizacion, 
                    ROW_NUMBER() OVER (PARTITION BY Primary_Key ORDER BY fecha_actualizacion DESC) AS rn 
                FROM clima
            ) AS sub
            WHERE sub.rn = 1
        );
    """


    for index, row in df_final.iterrows():
        cur.execute(sql, (row['lat'],row['lon'] ,row['Fecha y Hora'], row['Ciudad'], row['Temperatura C']
                        , row['Humedad Relativa %'], row['Velocidad de viento m/s'], row['Fecha_Actualizacion'],
                        row['Primary_Key']))
        
    cur.execute(sql_quitar_anteriores)

    # confirmar cambios
    conn.commit()

    # Close cursor and connection
    cur.close()
    conn.close()

