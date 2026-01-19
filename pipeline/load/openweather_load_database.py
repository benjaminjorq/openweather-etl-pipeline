import pandas as pd
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pathlib import Path
from datetime import datetime

# 1. Configuración
load_dotenv()
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_load_silver.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 2. Configuración de conexión DB

def get_db_engine():
    """Crea la conexión SQLAlchemy usando variables de entorno."""
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

# 3. Proceso Principal de Carga

def start_database_load():
    logging.info("Iniciando carga en Base de Datos PostgreSQL")
    
    # 4. Localizar partición del día actual

    now = datetime.now()
    todays_path = Path("data/silver") / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    
    if not todays_path.exists():
        logging.warning("No se encontró carpeta Silver del día actual.")
        return

    try:
        files = list(todays_path.glob("clean_data_*.csv"))
        if not files: 
            logging.warning("Carpeta del día vacía.")
            return
            
        latest_csv = max(files, key=lambda f: f.stat().st_mtime)
        logging.info(f"Cargando archivo: {latest_csv.name}")

        # 5. Lectura del csv

        df = pd.read_csv(latest_csv)
        
        # 6. Inserción en Base de Datos

        engine = get_db_engine()
        
        df.to_sql("weather_silver_table", engine, if_exists="append", index=False, schema="processed")

        logging.info(f"Carga exitosa: {len(df)} registros insertados en PostgreSQL.")
        
    except Exception as e:
        logging.error(f"Error durante el proceso de carga: {e}")

if __name__ == "__main__":
    start_database_load()