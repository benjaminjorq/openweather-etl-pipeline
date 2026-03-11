import pandas as pd
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
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
        exit()

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
            df.to_sql(name="weather_silver_table", con=connection, if_exists="append", index=False, schema="public", method="multi")
        logging.info(f"Carga exitosa: {len(df)} registros insertados en PostgreSQL.")
        
    except Exception as e:
        logging.error(f"Error durante el proceso de carga: {e}")

if __name__ == "__main__":
    start_database_load()