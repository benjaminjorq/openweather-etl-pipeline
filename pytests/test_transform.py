import pytest
import pandas as pd
from src.transform.openweather_batch_transform import clean_and_normalize

@pytest.fixture
def raw_dataset():
    """
    Simula un set de datos sucios con errores comunes (espacios, nulos, formatos)

    """
    raw_data = {
        "city": ["  Concepcion  ", "Santiago"],     
        "temperature_c": ["15.5", 25.0],            
        "humidity_pct": ["80", 60],                 
        "wind_speed_ms": ["10", 5],                 
        "country": [None, "CL"],                    
        "weather_desc": ["heavy rain", "Clear"],    
        "processed_timestamp": ["2026-01-20 12:00:00", "2026-01-20 12:00:00"]
    }
    return pd.DataFrame(raw_data)


def test_string_cleaning(raw_dataset):
    """
    Verifica la limpieza de strings: eliminación de espacios, manejo de nulos y capitalización.

    """
    processed_df = clean_and_normalize(raw_dataset)
    
    assert processed_df.iloc[0]["city"] == "Concepcion"
    assert processed_df.iloc[0]["country"] == "Unknown"
    assert processed_df.iloc[0]["weather_desc"] == "Heavy rain"

def test_numeric_casting(raw_dataset):
    """
    Valida que los textos numéricos se conviertan correctamente a tipos float o int.

    """
    processed_df = clean_and_normalize(raw_dataset)
    
    assert processed_df.iloc[0]["temperature_c"] == 15.5
    assert isinstance(processed_df.iloc[0]["temperature_c"], float)
    assert processed_df.iloc[0]["humidity_pct"] == 80

def test_date_conversion(raw_dataset):
    """
    Comprueba que processed_timestamp se transforme al tipo datetime.
    
    """
    processed_df = clean_and_normalize(raw_dataset)
    
    assert processed_df["processed_timestamp"].dtype == "datetime64[ns]"
