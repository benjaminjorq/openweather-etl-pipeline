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

# 3. Configuración de conexión DB

def create_db_engine():
    """Crea la conexión SQLAlchemy usando credenciales seguras.

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

# 4. Obtención de archivo Silver particionado (más reciente)

def get_latest_silver_file():
    """Obtiene el archivo CSV más reciente de la capa Silver del día actual.

    Returns:
        Path: Ruta del archivo CSV a cargar.

    Raises:
        FileNotFoundError: Si no se encuentra la partición o está vacía.
    """
    now = datetime.now()
    todays_path = SILVER_FOLDER / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"

    if not todays_path.exists():
        logging.warning("No se encontró carpeta Silver del día actual.")
        raise FileNotFoundError("No se encontró la partición Silver del día actual")
    
    files = list(todays_path.glob("clean_weather_data_*.csv"))
    if not files:
        logging.error(f"Carpeta de partición vacía en: {todays_path}")
        raise FileNotFoundError("La carpeta del día no contiene archivos CSV.")
        
    return max(files, key=lambda f: f.stat().st_mtime)

# 5. Carga de datos a Gold

def load_to_gold(df: pd.DataFrame, engine) -> None:
    """
    Carga datos en la tabla staging y ejecuta transformaciones hacia la capa Gold.

    Args:
        df (pd.DataFrame): Datos procesados desde la capa Silver.
        engine (Engine): Motor de conexión a PostgreSQL.

    Raises:
        RuntimeError: Si ocurre un error durante la carga o transformación.
    """
    with engine.begin() as connection:
        df.to_sql(name="weather_silver_table", con=connection, if_exists="replace", index=False, schema="public", method="multi")

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
            INSERT INTO dwh.dim_time (full_date, hour, day, month, year, is_weekend)
            SELECT DISTINCT 
                processed_timestamp::TIMESTAMP,
                EXTRACT(HOUR FROM processed_timestamp::TIMESTAMP),
                EXTRACT(DAY FROM processed_timestamp::TIMESTAMP),
                EXTRACT(MONTH FROM processed_timestamp::TIMESTAMP),
                EXTRACT(YEAR FROM processed_timestamp::TIMESTAMP),
                CASE WHEN EXTRACT(ISODOW FROM processed_timestamp::TIMESTAMP) IN (6, 7) THEN TRUE ELSE FALSE END
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

def start_database_load():
    """
    Ejecuta el proceso de carga de datos desde la capa Silver hacia PostgreSQL.

    Raises:
        RuntimeError: Si no hay datos disponibles o falla el pipeline.
    """
    logging.info("Iniciando proceso de carga a PostgreSQL")
    
    try:
        latest_csv = get_latest_silver_file()
        logging.info(f"Cargando archivo: {latest_csv.name}")

        df = pd.read_csv(latest_csv)
        
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
    
# 7. Ejecución de Carga en BD

if __name__ == "__main__":
    start_database_load()