
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.validations.data_quality import (validate_schema_and_volume,apply_data_quality)

BASE_DIR = Path("/opt/airflow")
LOG_DIR = BASE_DIR / "logs"
BRONZE_FOLDER = BASE_DIR / "data/bronze"
SILVER_FOLDER = BASE_DIR / "data/silver"

LOG_DIR.mkdir(parents=True, exist_ok=True)
SILVER_FOLDER.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_transform.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def cast_numeric_columns(df):
    """Convierte las columnas de texto a tipo numérico (float). """

    df["temperature_c"] = pd.to_numeric(df["temperature_c"], "coerce")
    df["humidity_pct"] = pd.to_numeric(df["humidity_pct"], "coerce")
    df["wind_speed_ms"] = pd.to_numeric(df["wind_speed_ms"], "coerce")

    return df

def normalize_string_columns(df):
    """Limpia los espacios en las cadenas, capitaliza y rellena nulos con "Unknown"""

    df["city"] = df["city"].astype(str).str.strip()
    df["country"] = df["country"].fillna("Unknown").astype(str).str.strip()
    df["weather_desc"] = df["weather_desc"].fillna("").astype(str).str.strip().str.capitalize()

    return df

def normalize_datetime_columns(df):
    """Convierte la fecha de procesamiento de string a datetime de Pandas."""

    df["processed_timestamp"] = pd.to_datetime(df["processed_timestamp"])

    return df

def clean_and_normalize(df):
    """Estandariza los tipos de datos y formatos del DataFrame"""

    df = cast_numeric_columns(df)
    df = normalize_string_columns(df)
    df = normalize_datetime_columns(df)

    return df

def start_transformation_process(execution_date):
    """
    Orquesta la transformación desde la ingesta JSON cruda hasta el CSV particionado Silver.
    
    Args:
        execution_date (str): Fecha de ejecución en formato YYYYMMDDTHHMMSS.
        
    Raises:
        FileNotFoundError: Si no existe el archivo JSON de la capa Bronze.
    """

    logging.info(f"Transformando datos a Silver para la fecha: {execution_date}")
    
    bronze_file_path = BRONZE_FOLDER / f"raw_weather_data_{execution_date}.json"
    execution_datetime = datetime.strptime(execution_date, "%Y%m%dT%H%M%S")
    
    if not bronze_file_path.exists():
        raise FileNotFoundError(f"No existe el archivo de ingesta: {bronze_file_path}")
        
    with open(bronze_file_path, "r", encoding="utf-8") as file:
        bronze_raw_data = json.load(file)

    df = pd.json_normalize(bronze_raw_data)

    df = df.rename(columns={
        "city_metadata.name": "city",
        "weather_raw_data.sys.country": "country",
        "weather_raw_data.main.temp": "temperature_c",
        "weather_raw_data.main.feels_like": "feels_like_c",
        "weather_raw_data.main.humidity": "humidity_pct",
        "weather_raw_data.main.pressure": "pressure_hpa",
        "weather_raw_data.wind.speed": "wind_speed_ms",
    })
    
    if "weather_raw_data.weather" in df.columns:
        df["weather_desc"] = df["weather_raw_data.weather"].str[0].str.get("description")
        
    if "pollution_raw_data.list" in df.columns:
        pollution_series = df["pollution_raw_data.list"].str[0]
        df["aqi"] = pollution_series.str["main"].str["aqi"]
        df["co_level"] = pollution_series.str["components"].str["co"]
        df["no2_level"] = pollution_series.str["components"].str["no2"]
        df["pm2_5_level"] = pollution_series.str["components"].str["pm2_5"]
        df["pm10_level"] = pollution_series.str["components"].str["pm10"]
        df["o3_level"] = pollution_series.str["components"].str["o3"]
        
    df["processed_timestamp"] = execution_datetime

    expected_columns = [
        "city", "country", "temperature_c", "feels_like_c", "humidity_pct", 
        "pressure_hpa", "wind_speed_ms", "weather_desc", "aqi", "co_level", 
        "no2_level", "pm2_5_level", "pm10_level", "o3_level", "processed_timestamp"
    ]
    df = df.reindex(columns=expected_columns)

    validate_schema_and_volume(df, expected_columns)

    logging.info(f"Total de Filas antes de Transformar: {len(df)}")
    logging.info(f"Tipos de Datos antes de Transformar:\n{df.dtypes}")

    df = clean_and_normalize(df)
    df = apply_data_quality(df)

    silver_output_directory = SILVER_FOLDER / f"year={execution_datetime.year}" / f"month={execution_datetime.month:02d}" / f"day={execution_datetime.day:02d}"
    silver_output_directory.mkdir(parents=True, exist_ok=True)
    
    silver_filename = f"clean_weather_data_{execution_date}.csv"
    df.to_csv(silver_output_directory / silver_filename, index=False)
    
    logging.info(f"Transformación exitosa. Archivo guardado en Silver: {silver_filename}")

if __name__ == "__main__":

    test_date = datetime.now().strftime('%Y%m%dT%H%M%S')
    start_transformation_process(test_date)