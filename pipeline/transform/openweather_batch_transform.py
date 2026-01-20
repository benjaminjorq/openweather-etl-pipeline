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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_transform.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 2. Función para obtener archivo fuente

def get_latest_bronze_file():
    """Retorna la ruta del archivo JSON más reciente en Bronze."""
    files = list(BRONZE_FOLDER.glob("*.json"))
    if not files:
        raise FileNotFoundError("Directorio Bronze vacío.")
    return max(files, key=lambda f: f.stat().st_mtime)  #Lee el último registro desde Bronze

# 3. Función de Lógica de Negocio y Limpieza

def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica casting de tipos, limpieza de strings y filtros de calidad."""

    # Casting de numéricos
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["humidity_pct"] = pd.to_numeric(df["humidity_pct"], errors="coerce")
    df["wind_speed_ms"] = pd.to_numeric(df["wind_speed_ms"], errors="coerce")
    
    # Normalización de Strings

    df["city"] = df["city"].astype(str).str.strip()
    df["country"] = df["country"].fillna("Unknown").astype(str).str.strip()
    df["weather_desc"] = df["weather_desc"].astype(str).str.strip().str.capitalize()

    # Conversión de Fecha 

    df["processed_timestamp"] = pd.to_datetime(df["processed_timestamp"])
    
    # Filtros de Calidad

    df = df.drop_duplicates(subset=["city", "processed_timestamp"])
    df = df.dropna(subset=["city", "temperature_c"])
    
    # Filtro de Rango 

    df = df[(df["temperature_c"] > -90) & (df["temperature_c"] < 60)]
    
    return df

# 4. Proceso Principal de Transformación

def start_transformation_process():
    logging.info("Iniciando Proceso de Transformación")
    
    try:
        file_path = get_latest_bronze_file()
        logging.info(f"Procesando archivo: {file_path.name}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        clean_rows = []
        
        # 5. Aplanamiento de datos (Flattening)

        for record in raw_data:
            try:
                meta = record.get("city_metadata", {}) # Extrae los metadatos de la ciudad (nombre, lat, lon)
                weather = record.get("weather_raw_data", {}) # Extrae raw data del Clima
                pollution = record.get("pollution_raw_data", {}).get("list", [{}])[0] # Extrae raw data de Polución (usa fallback para prevenir un IndexError)
                
                row = {
                    # Datos Clima (weather)
                    "city": meta.get("name"),
                    "country": weather.get("sys", {}).get("country"),
                    "temperature_c": weather.get("main", {}).get("temp"),
                    "feels_like_c": weather.get("main", {}).get("feels_like"),
                    "humidity_pct": weather.get("main", {}).get("humidity"),
                    "pressure_hpa": weather.get("main", {}).get("pressure"),
                    "wind_speed_ms": weather.get("wind", {}).get("speed"),
                    "weather_desc": weather.get("weather", [{}])[0].get("description"),

                    # Datos Polución (pollution)
                    "aqi": pollution.get("main", {}).get("aqi"),
                    "co_level": pollution.get("components", {}).get("co"),      
                    "no2_level": pollution.get("components", {}).get("no2"),    
                    "o3_level": pollution.get("components", {}).get("o3"),
                    "pm2_5_level": pollution.get("components", {}).get("pm2_5"),
                    "pm10_level": pollution.get("components", {}).get("pm10"),
                    "processed_timestamp": datetime.now()
                }
                clean_rows.append(row)
            except Exception as e:
                logging.warning(f"Error procesando registro individual: {e}")

        # 6. Creación y Persistencia del DataFrame

        if clean_rows:
            df = pd.DataFrame(clean_rows)
            df = clean_and_normalize(df)
            
            if df.empty:
                logging.warning("Todos los datos fueron filtrados por calidad.")
                return

            # Particionamiento por fecha

            now = datetime.now()
            output_dir = SILVER_FOLDER / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"clean_weather_data_{now.strftime('%H_%M_%S')}.csv"
            df.to_csv(output_dir / filename, index=False)
            
            logging.info(f"Transformación completada. CSV guardado: {filename}")
        else:
            logging.warning("No se generaron registros válidos para transformar.")

    except Exception as e:
        logging.critical(f"Fallo crítico en transformación: {e}")

if __name__ == "__main__":
    start_transformation_process()