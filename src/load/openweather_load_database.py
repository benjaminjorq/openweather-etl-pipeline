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

def get_db_engine():
    """
    Objetivo: Crea la conexión SQLAlchemy usando credenciales seguras.
    Solución de Fallos: 
    - Connection refused: Validar contenedor PostgreSQL o credenciales en .env.
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
        raise RuntimeError(f"No se pudo conectar a la base de datos: {e}")

# 4. Proceso Principal de Carga

def start_database_load():
    """
    Objetivo: Localiza la última partición Silver
    y carga el DataFrame hacia PostgreSQL.
    """
    logging.info("Iniciando carga en Base de Datos PostgreSQL")
    
    # 5. Localizar partición del día actual

    now = datetime.now()
    todays_path = SILVER_FOLDER / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    
    if not todays_path.exists():
        logging.warning("No se encontró carpeta Silver del día actual.")
        return

    try:
        files = list(todays_path.glob("clean_weather_data_*.csv"))
        if not files: 
            logging.warning("Carpeta del día vacía.")
            return
            
        latest_csv = max(files, key=lambda f: f.stat().st_mtime)
        logging.info(f"Cargando archivo: {latest_csv.name}")

        # 6. Lectura del CSV

        df = pd.read_csv(latest_csv)
        
        # 7. Inserción en Base de Datos

        engine = get_db_engine()

        with engine.begin() as connection:
            df.to_sql(name="weather_silver_table", con=connection, if_exists="replace", index=False, schema="public", method="multi")
            logging.info(f"Carga Silver exitosa: {len(df)} registros insertados en PostgreSQL.")

            # 8. Proceso ELT: Inserción de datos en Gold (UPSERT)

            logging.info("Iniciando traspaso automático hacia capa Gold (Dimensiones y Hechos)")
            
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
                JOIN dwh.dim_location AS l ON s.city = l.city AND s.country = l.country
                JOIN dwh.dim_weather_condition AS c ON s.weather_desc = c.description
                JOIN dwh.dim_time AS t ON s.processed_timestamp::TIMESTAMP = t.full_date
                ON CONFLICT (location_id, time_id) DO NOTHING;
                """

            connection.execute(text(query)) # Text para evitar error con SQLAlchemy 2.x
            logging.info("Traspaso a Capa Gold completado con éxito")
            logging.info("Data Warehouse está actualizado")
        
    except Exception as e:
        logging.error(f"Error durante el proceso de carga: {e}")
        raise

# 9. Ejecución de Carga en BD

if __name__ == "__main__":
    start_database_load()