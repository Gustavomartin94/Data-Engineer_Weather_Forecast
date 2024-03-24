CREATE TABLE clima (
    Indice INT,
    Latitud DECIMAL(9,6),
    Longitud DECIMAL(9,6),
    Fecha_pronost TIMESTAMP WITH TIME zone,
    Ciudad VARCHAR(100),
    Temperatura_C FLOAT,
    Humedad_porcentaje FLOAT,
    Velocidad_de_viento FLOAT,
    Fecha_Actualizacion TIMESTAMP,
    Primary_Key VARCHAR(100),
    PRIMARY KEY (Primary_Key)
);


