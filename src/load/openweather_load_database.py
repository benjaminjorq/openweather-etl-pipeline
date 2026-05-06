
"""
Módulo de Carga (Gold Layer) - OpenWeather ETL.

Lee el archivo CSV de la capa Silver correspondiente a la fecha de ejecución
y lo carga a PostgreSQL. Primero inserta los datos en una tabla de staging,
luego ejecuta las transformaciones SQL hacia el Data Warehouse en esquema estrella.

"""


import pandas as pd
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

# 1. Configuración de Rutas

load_dotenv()

BASE_DIR = Path("/opt/airflow")
LOG_DIR = BASE_DIR / "logs"
SILVER_FOLDER = BASE_DIR / "data/silver"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 2. Configuración de Logs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_load_silver.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 3. Configuración de Conexión a la Base de Datos

def create_db_engine():
    """
    Crea el motor de conexión SQLAlchemy usando credenciales desde variables de entorno.

    Returns:
        Engine: Objeto engine de conexión a PostgreSQL.

    Raises:
        RuntimeError: Si fallan las credenciales o la conexión es rechazada.
    """
    try:
        USER = os.getenv('DB_USER')
        PASSWORD = os.getenv('DB_PASSWORD')
        HOST = os.getenv('DB_HOST')
        PORT = os.getenv('DB_PORT')
        DBNAME = os.getenv('DB_NAME')

        url = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
        
        return create_engine(url)

    except Exception as e:
        logging.critical(f"Error configurando motor de base de datos: {e}")
        raise RuntimeError(f"No se pudo establecer la conexión a la base de datos: {e}")

# 4. Obtención del Archivo Silver Particionado

def get_target_silver_file(execution_date: str):
    """
    Obtiene el archivo CSV de la capa Silver correspondiente a la fecha de ejecución.

    Args:
        execution_date (str): Fecha lógica YYYYMMDDTHHMMSS.

    Returns:
        Path: Ruta exacta del archivo CSV a cargar.

    Raises:
        FileNotFoundError: Si no se encuentra la partición o el archivo.
    """

    execution_dt = datetime.strptime(execution_date, "%Y%m%dT%H%M%S")
    target_path = SILVER_FOLDER / f"year={execution_dt.year}" / f"month={execution_dt.month:02d}" / f"day={execution_dt.day:02d}" / f"clean_weather_data_{execution_date}.csv"

    if not target_path.exists():
        logging.warning(f"No se encontró el archivo particionado: {target_path}")
        raise FileNotFoundError(f"Falta el archivo Silver para la fecha {execution_date}")
        
    return target_path

# 5. Carga a Staging y Traspaso al Data Warehouse

def load_to_gold(df: pd.DataFrame, engine) -> None:
    """
    Carga los datos en la tabla de staging y ejecuta las transformaciones SQL
    hacia las tablas del Data Warehouse en esquema estrella.

    El proceso inserta primero en staging, luego actualiza las dimensiones
    (location, weather_condition, time) y finalmente la tabla de hechos.

    Args:
        df (pd.DataFrame): Datos procesados desde la capa Silver.
        engine (Engine): Motor de conexión a PostgreSQL.

    Raises:
        RuntimeError: Si ocurre un error durante la carga o transformación.
    """

    with engine.begin() as connection:

        df.to_sql(name="weather_silver_table", con=connection, if_exists="append", index=False, schema="public", method="multi")

        logging.info(f"Carga a Staging exitosa: {len(df)} registros insertados.")
        logging.info("Actualizando datos en Gold")
        
        query = """
            -- 1. Actualizar Dimensión Location
            INSERT INTO dwh.dim_location (city, country)
            SELECT DISTINCT city, country FROM public.weather_silver_table
            ON CONFLICT (city, country) DO NOTHING;

            -- 2. Actualizar Dimensión Weather Condition
            INSERT INTO dwh.dim_weather_condition (description)
            SELECT DISTINCT weather_desc FROM public.weather_silver_table
            ON CONFLICT (description) DO NOTHING;

            -- 3. Actualizar Dimensión Time
            INSERT INTO dwh.dim_time (full_date, hour, day, month, year)
            SELECT DISTINCT 
                processed_timestamp::TIMESTAMP,
                EXTRACT(HOUR FROM processed_timestamp::TIMESTAMP),
                EXTRACT(DAY FROM processed_timestamp::TIMESTAMP),
                EXTRACT(MONTH FROM processed_timestamp::TIMESTAMP),
                EXTRACT(YEAR FROM processed_timestamp::TIMESTAMP)
            FROM public.weather_silver_table
            ON CONFLICT (full_date) DO NOTHING;

            -- 4. Tabla de Hechos 
            INSERT INTO dwh.fact_weather_metrics (
                location_id, time_id, condition_id, aqi_id,
                feels_like_c, pressure_hpa,
                temperature_c, humidity_pct, wind_speed_ms, 
                pm2_5_level, pm10_level, co_level, no2_level, o3_level
            )
            SELECT 
                l.location_id, t.time_id, c.condition_id, s.aqi, 
                s.feels_like_c, s.pressure_hpa, 
                s.temperature_c, s.humidity_pct, s.wind_speed_ms,
                s.pm2_5_level, s.pm10_level, s.co_level, s.no2_level, s.o3_level
            FROM public.weather_silver_table AS s
            JOIN dwh.dim_location AS l 
                ON s.city = l.city AND s.country = l.country
            JOIN dwh.dim_weather_condition AS c 
                ON s.weather_desc = c.description
            JOIN dwh.dim_time AS t 
                ON s.processed_timestamp::TIMESTAMP = t.full_date
            ON CONFLICT (location_id, time_id) DO NOTHING;
        """
        connection.execute(text(query))
        logging.info("Traspaso a Capa Gold completado con éxito. Data Warehouse actualizado.")

# 6. Orquestador del pipeline

def start_database_load(execution_date: str):
    """
    Ejecuta el proceso completo de carga desde la capa Silver hacia PostgreSQL.

    Args:
        execution_date (str): Fecha de ejecución lógica en formato YYYYMMDDTHHMMSS.

    Raises:
        RuntimeError: Si no hay datos disponibles o falla alguna etapa del pipeline.
    """
    logging.info("Iniciando proceso de carga a PostgreSQL")
    
    try:
        target_csv = get_target_silver_file(execution_date)
        logging.info(f"Cargando archivo: {target_csv.name}")

        df = pd.read_csv(target_csv)
        
        if df.empty:
            logging.warning("El archivo CSV está vacío.")
            return

        engine = create_db_engine()
        load_to_gold(df, engine)
        
    except FileNotFoundError as e:
        logging.critical(f"Falta archivo crítico: {e}")
        raise RuntimeError("No hay datos nuevos para procesar.") from e
        
    except Exception as e:
        logging.critical("Fallo durante el proceso de carga.")
        raise RuntimeError("Falla en la etapa de carga.") from e
    
# 7. Ejecución 

if __name__ == "__main__":

    # Prueba manual: Ejecuta la carga directamente sin Airflow.
    # Busca el archivo Silver con la fecha actual y lo inserta en PostgreSQL.

    test_date = datetime.now().strftime('%Y%m%dT%H%M%S')
    start_database_load(execution_date=test_date)

