import pytest
import pandas as pd
from src.transform.openweather_batch_transform import clean_and_normalize

# 1. Creamos un DataFrame sucio para probar la función clean_and_normalize

def create_raw_dataset():
    """Simula un set de datos sucios con errores comunes (espacios, nulos, formatos)"""
    raw_data = {
        "city": ["  Concepcion  ", "Santiago"],     # Espacios extra
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
"""Verifica la limpieza de strings: eliminación de espacios, manejo de nulos y capitalización."""
    raw_df = create_raw_dataset()
    processed_df = clean_and_normalize(raw_df)
    
    assert processed_df.iloc[0]["city"] == "Concepcion"
    assert processed_df.iloc[0]["country"] == "Unknown"
    assert processed_df.iloc[0]["weather_desc"] == "Heavy rain"

def test_numeric_casting():
"""Valida que los textos numéricos se conviertan correctamente a tipos float o int."""
    raw_df = create_raw_dataset()
    processed_df = clean_and_normalize(raw_df)
    
    assert processed_df.iloc[0]["temperature_c"] == 15.5
    assert isinstance(processed_df.iloc[0]["temperature_c"], float)
    assert processed_df.iloc[0]["humidity_pct"] == 80

def test_date_conversion():
"""Comprueba que processed_tinmestamp se transforme al tipo datetime."""
    raw_df = create_raw_dataset()
    processed_df = clean_and_normalize(raw_df)
    
    assert processed_df["processed_timestamp"].dtype == "datetime64[ns]"
