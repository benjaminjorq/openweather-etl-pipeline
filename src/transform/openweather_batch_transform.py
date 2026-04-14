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

# 3. Función para obtener archivo fuente

def get_latest_bronze_file():
    """Obtiene el archivo JSON más reciente de la capa Bronze.

    Returns:
        Path: Ruta del archivo Bronze más reciente.

    Raises:
        FileNotFoundError: Si no existen archivos en Bronze (indica fallo en la ingesta).
    """
    files = list(BRONZE_FOLDER.glob("*.json"))
    if not files:
        raise FileNotFoundError("Directorio Bronze vacío. No hay datos para transformar")
    return max(files, key=lambda f: f.stat().st_mtime)  

# 4. Función de Lógica de Negocio y Limpieza

def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia, normaliza y valida los datos.

    Args:
        df (pd.DataFrame): DataFrame proveniente de Bronze.

    Returns:
        pd.DataFrame: DataFrame procesado, limpio y validado.
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
    
    # Filtros de Calidad y Validación

    initial_rows = len(df)
    df = df.drop_duplicates(subset=["city", "processed_timestamp"])
    logging.info(f"Se eliminaron : {initial_rows - len(df)} filas duplicadas")

    rows_before_dropna = len(df)
    df = df.dropna(subset=["city", "temperature_c"])
    logging.info(f"Se eliminaron : {rows_before_dropna - len(df)} valores faltantes")
    
    # Filtro de Rango

    df = df[(df["temperature_c"] > -90) & (df["temperature_c"] < 60)]

    return df

# 5. Proceso Principal de Transformación

def start_transformation_process():
    """
    Orquesta la extracción, aplanamiento, filtrado y particionado.

    Raises:
        RuntimeError: Si ocurre un fallo crítico durante el proceso de transformación.

    """
    logging.info("Iniciando proceso de Transformación")

    try:
        file_path = get_latest_bronze_file()
        logging.info(f"Procesando archivo: {file_path.name}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if not raw_data:
            logging.warning("El JSON está vacío.")
            return

        # 1. Flatten del JSON anidado

        df = pd.json_normalize(raw_data)

        # 2. Renombrar columnas 

        df = df.rename(columns={
            "city_metadata.name": "city",
            "weather_raw_data.sys.country": "country",
            "weather_raw_data.main.temp": "temperature_c",
            "weather_raw_data.main.feels_like": "feels_like_c",
            "weather_raw_data.main.humidity": "humidity_pct",
            "weather_raw_data.main.pressure": "pressure_hpa",
            "weather_raw_data.wind.speed": "wind_speed_ms",
        })

        # 3. Extracción del Clima

        if "weather_raw_data.weather" in df.columns:
            df["weather_desc"] = df["weather_raw_data.weather"].str[0].str.get("description")

        # 4. Extracción de Polución

        if "pollution_raw_data.list" in df.columns:
            pollution = df["pollution_raw_data.list"].str[0] 
            
            df["aqi"] = pollution.str["main"].str["aqi"]
            df["co_level"] = pollution.str["components"].str["co"]
            df["no2_level"] = pollution.str["components"].str["no2"]
            df["o3_level"] = pollution.str["components"].str["o3"]
            df["pm2_5_level"] = pollution.str["components"].str["pm2_5"]
            df["pm10_level"] = pollution.str["components"].str["pm10"]

        # 5. Timestamp del batch

        df["processed_timestamp"] = datetime.now().replace(second=0, microsecond=0)

        output_columns = [
            "city", "country", "temperature_c", "feels_like_c", "humidity_pct",
            "pressure_hpa", "wind_speed_ms", "weather_desc", "aqi", "co_level",
            "no2_level", "o3_level", "pm2_5_level", "pm10_level", "processed_timestamp"
        ]
        
        df = df.reindex(columns=output_columns)

        # 6. Limpieza y validación del DataFrame

        df = clean_and_normalize(df)
        
        if df.empty:
            logging.warning("Todos los datos fueron filtrados por calidad.")
            return

        # 7. Particionamiento por fecha

        now = datetime.now()
        output_dir = SILVER_FOLDER / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"clean_weather_data_{now.strftime('%H_%M_%S')}.csv"
        df.to_csv(output_dir / filename, index=False)
        
        logging.info(f"Transformación completada. CSV guardado: {filename}")

    except Exception as e:
        logging.critical(f"Fallo crítico en transformación: {e}")
        raise RuntimeError("Error en proceso de transformación") from e

if __name__ == "__main__":
    start_transformation_process()