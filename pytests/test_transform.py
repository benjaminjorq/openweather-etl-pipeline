import pytest
import pandas as pd
from pipeline.transform.openweather_batch_transform import clean_and_normalize

# 1. Creamos un DataFrame sucio para probar la función clean_and_normalize

def create_raw_dataset():
    """Simula un set de datos sucios con errores comunes (espacios, nulos, formatos)"""
    raw_data = {
        "city": ["  Concepcion  ", "Santiago"],      # Espacios extra
        "temperature_c": ["15.5", 25.0],            # Mezcla texto/número
        "humidity_pct": ["80", 60],                 # Mezcla texto/número
        "wind_speed_ms": ["10", 5],                 # Mezcla texto/número
        "country": [None, "CL"],                    # Nulo explícito
        "weather_desc": ["heavy rain", "Clear"],    # Minúsculas inconsistentes
        "processed_timestamp": ["2026-01-20 12:00:00", "2026-01-20 12:00:00"]
    }
    return pd.DataFrame(raw_data)

# 2. Pruebas

def test_string_cleaning():
    # 1. Creamos datos sucios
    raw_df = create_raw_dataset()
    
    # 2. Ejecutamos la función de limpieza
    processed_df = clean_and_normalize(raw_df)
    
    # 3. Verificar que los textos quedaron limpios
    
    # ¿Se quitaron los espacios extra?
    assert processed_df.iloc[0]["city"] == "Concepcion"
    
    # ¿Se rellenó el país faltante con 'Unknown'?
    assert processed_df.iloc[0]["country"] == "Unknown"
    
    # ¿Se corrigió la mayúscula inicial?
    assert processed_df.iloc[0]["weather_desc"] == "Heavy rain"

def test_numeric_casting():
    # 1. Creamos datos sucios
    raw_df = create_raw_dataset()
    
    # 2. Ejecutamos la función de limpieza
    processed_df = clean_and_normalize(raw_df)
    
    # 3. Verificamos la conversión a números

    # ¿El texto "15.5" pasó a ser el número decimal 15.5?
    assert processed_df.iloc[0]["temperature_c"] == 15.5
    assert isinstance(processed_df.iloc[0]["temperature_c"], float)
    
    # ¿El texto "80" pasó a ser el entero 80?
    assert processed_df.iloc[0]["humidity_pct"] == 80

def test_date_conversion():
    # 1. Creamos datos sucios
    raw_df = create_raw_dataset()
    
    # 2. Ejecutamos la función de limpieza
    processed_df = clean_and_normalize(raw_df)
    
    # 3. Verificamos processed_timestamp sea una fecha válida
    
    # Revisamos que la columna tenga el formato de fecha (datetime) correcto
    assert processed_df["processed_timestamp"].dtype == "datetime64[ns]"
