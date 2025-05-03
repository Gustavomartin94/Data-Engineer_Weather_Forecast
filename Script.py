# EXTRACTION AND TRANSFORMATION

import requests
import pandas as pd
from datetime import datetime
from config import KEY
from constants import cities_coordinates

# Define the base URL for the OpenWeatherMap API
base_url = "http://api.openweathermap.org/data/2.5/forecast"

# Coordinates of desired locations
coordinates = [(city, coord[0], coord[1]) for city, coord in cities_coordinates.items()]

# Define query parameters
parameters = {
    'units': 'metric',  # To get metric units
    'appid': KEY,  # Your OpenWeatherMap API key
    'cnt': 4  # Number of forecast periods to receive
}

# List to store data for each location
data_list = []

# Query the API for each location
for city, lat, lon in coordinates:
    # Build the query URL
    query_params = parameters.copy()
    query_params['lat'] = lat
    query_params['lon'] = lon

    # Make a GET request to the OpenWeatherMap API
    response = requests.get(base_url, params=query_params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response to JSON format
        weather_data = response.json()

        # Extract relevant future forecast data
        forecast_data = weather_data['list']
        for forecast in forecast_data:
            forecast_time = forecast['dt_txt']
            temperature = forecast['main']['temp']
            humidity = forecast['main']['humidity']
            wind_speed = forecast['wind']['speed']
            # Add data to the list
            data_list.append({
                'lat': lat,
                'lon': lon,
                'Ciudad': city,
                'Fecha y Hora': forecast_time,
                'Temperatura C': temperature,
                'Humedad Relativa %': humidity,
                'Velocidad de viento m/s': wind_speed,
            })
    else:
        print(f"Error retrieving data for {symbol}: {response.status_code}")

# Create a DataFrame from the collected data
df_final = pd.DataFrame(data_list)

# Convert the date and time string to datetime format
df_final['Fecha y Hora'] = pd.to_datetime(df_final['Fecha y Hora'])

# Format the 'Fecha y Hora' column to display only date and time (without seconds)
df_final['Fecha y Hora'] = df_final['Fecha y Hora'].dt.strftime('%Y-%m-%d %H:%M')

# Add the update date column
fecha_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_final['Fecha_Actualizacion'] = fecha_actualizacion

# Add a primary key and index column
df_final['Primary_Key'] = df_final['Ciudad'] + ' ' + df_final['Fecha y Hora']

# Reorder the columns
df_final = df_final[['lat', 'lon', 'Fecha y Hora', 'Ciudad', 'Temperatura C', 'Humedad Relativa %', 'Velocidad de viento m/s', 'Fecha_Actualizacion', 'Primary_Key']]

#-----------------------------------------------------------------------------------------------

# LOADING TO REDSHIFT

import psycopg2
from config import dbname, user, clave, host, port2

# Credentials
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=clave,
    host=host,
    port=port2
)

# Create cursor
cur = conn.cursor()

# Insert data
sql = "INSERT INTO clima VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);"

# Update data, deleting previous entries when Primary_Key is duplicated due to updates
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
    cur.execute(sql, (row['lat'], row['lon'], row['Fecha y Hora'], row['Ciudad'], row['Temperatura C'],
                      row['Humedad Relativa %'], row['Velocidad de viento m/s'], row['Fecha_Actualizacion'],
                      row['Primary_Key']))
    
cur.execute(sql_quitar_anteriores)

# Confirm changes
conn.commit()

# Close cursor and connection
cur.close()
conn.close()
