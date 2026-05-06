
"""
Módulo de Transformación (Silver Layer) - OpenWeather ETL.

Normaliza y limpia los archivos JSON de la capa Bronze.
Aplica reglas de calidad de datos.

"""

import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# 1. Configuración de Rutas

BASE_DIR = Path("/opt/airflow")
LOG_DIR = BASE_DIR / "logs"
BRONZE_FOLDER = BASE_DIR / "data/bronze"
SILVER_FOLDER = BASE_DIR / "data/silver"

LOG_DIR.mkdir(parents=True, exist_ok=True)
SILVER_FOLDER.mkdir(parents=True, exist_ok=True)

# 2. Configuración de Logs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_transform.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 3. Función de Limpieza y Normalización

def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """   
    Limpia, normaliza y valida los datos provenientes de la capa Bronze.

    Aplica casting de tipos numéricos, normalización de strings,
    conversión de fechas, eliminación de duplicados y filtros de rango.

    Args:
        df (pd.DataFrame): DataFrame con los datos crudos desde Bronze.

    Returns:
        pd.DataFrame: DataFrame procesado, limpio y validado listo para Silver.
    """

    logging.info(f"Total de Filas antes de Transformar: {len(df)}")
    logging.info(f"Tipos de Datos antes de Transformar: {df.dtypes}")

    # Casting de numéricos

    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["humidity_pct"] = pd.to_numeric(df["humidity_pct"], errors="coerce")
    df["wind_speed_ms"] = pd.to_numeric(df["wind_speed_ms"], errors="coerce")
    
    # Normalización de Strings

    df["city"] = df["city"].astype(str).str.strip()
    df["country"] = df["country"].fillna("Unknown").astype(str).str.strip()
    df["weather_desc"] = df["weather_desc"].fillna("").astype(str).str.strip().str.capitalize()

    # Conversión de Fecha 

    df["processed_timestamp"] = pd.to_datetime(df["processed_timestamp"])
    
    # Eliminación de duplicados

    initial_rows = len(df)
    df = df.drop_duplicates(subset=["city", "processed_timestamp"])
    logging.info(f"Se eliminaron : {initial_rows - len(df)} filas duplicadas")

    # Eliminación de valores nulos en columnas críticas

    rows_before_dropna = len(df)
    df = df.dropna(subset=["city", "temperature_c"])
    logging.info(f"Se eliminaron : {rows_before_dropna - len(df)} valores faltantes")
    
    # Filtro de rango de temperatura válida (-90°C a 60°C)

    df = df[(df["temperature_c"] > -90) & (df["temperature_c"] < 60)]

    return df

# 4. Proceso Principal de Transformación

def start_transformation_process(execution_date: str):
    """
    Función principal de transformación y limpieza.

    Lee el archivo JSON de Bronze correspondiente a la fecha de ejecución,
    aplana la estructura anidada, renombra columnas, extrae métricas de
    polución y clima, y exporta el resultado limpio a la capa Silver
    en formato CSV particionado por año, mes y día.

    Args:
        execution_date (str): Fecha exacta del archivo a procesar (YYYYMMDDTHHMMSS).

    Raises:
        FileNotFoundError: Si el archivo Bronze correspondiente no existe.
    """

    logging.info(f"Transformando datos a Silver para la fecha: {execution_date}")
    
    file_path = BRONZE_FOLDER / f"raw_weather_data_{execution_date}.json"
    execution_dt = datetime.strptime(execution_date, "%Y%m%dT%H%M%S")

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo a transformar: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # 1. Aplanar el JSON anidado a estructura tabular

    df = pd.json_normalize(raw_data)
    
    # 2. Renombrar columnas al esquema estándar del proyecto

    df = df.rename(columns={
        "city_metadata.name": "city",
        "weather_raw_data.sys.country": "country",
        "weather_raw_data.main.temp": "temperature_c",
        "weather_raw_data.main.feels_like": "feels_like_c",
        "weather_raw_data.main.humidity": "humidity_pct",
        "weather_raw_data.main.pressure": "pressure_hpa",
        "weather_raw_data.wind.speed": "wind_speed_ms",
    })

    # 3. Extraer campos anidados en listas (clima y polución)

    if "weather_raw_data.weather" in df.columns:
        df["weather_desc"] = df["weather_raw_data.weather"].str[0].str.get("description")
    
    if "pollution_raw_data.list" in df.columns:
        pollution = df["pollution_raw_data.list"].str[0]
        df["aqi"] = pollution.str["main"].str["aqi"]
        df["co_level"] = pollution.str["components"].str["co"]
        df["no2_level"] = pollution.str["components"].str["no2"]
        df["pm2_5_level"] = pollution.str["components"].str["pm2_5"]
        df["o3_level"] = pollution.str["components"].str["o3"]
        df["pm10_level"] = pollution.str["components"].str["pm10"]

    df["processed_timestamp"] = execution_dt
    
    # 4. Reordenar columnas al esquema final

    target_columns = [
        "city", "country", "temperature_c", "feels_like_c", "humidity_pct", 
        "pressure_hpa", "wind_speed_ms", "weather_desc", "aqi", "co_level", 
        "no2_level", "pm2_5_level", "pm10_level", "o3_level", "processed_timestamp" 
    ]
    
    df = df.reindex(columns=target_columns)
    
    # 5. Limpieza y normalización de datos 

    df = clean_and_normalize(df)

    # 6. Guardar CSV particionado por año, mes y día (estilo Hive)

    output_dir = SILVER_FOLDER / f"year={execution_dt.year}" / f"month={execution_dt.month:02d}" / f"day={execution_dt.day:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"clean_weather_data_{execution_date}.csv"
    df.to_csv(output_dir / filename, index=False)
    logging.info(f"Guardado en Silver: {filename}")

# 5. Ejecución

if __name__ == "__main__":

    # Prueba manual: Ejecuta la transformación directamente sin Airflow.
    # Busca el archivo Bronze con la fecha actual y lo procesa hacia Silver.

    test_date = datetime.now().strftime('%Y%m%dT%H%M%S')
    start_transformation_process(execution_date=test_date)