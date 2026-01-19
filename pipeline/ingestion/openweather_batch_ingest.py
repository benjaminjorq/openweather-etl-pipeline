import requests
import json
import logging
import yaml
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# 1. Configuración de Entorno y Rutas

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

LOG_DIR = Path("logs")
BRONZE_FOLDER = Path("data/bronze")
CITIES_FILE = Path("config/cities.yaml")

# 2. Setup Inicial (Directorios y Logs)

LOG_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_FOLDER.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_ingestion.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 3. Función Auxiliar: Cargar Ciudades

def load_cities_config():
    """
    Lee el archivo YAML de configuración.
    """
    if not CITIES_FILE.exists():
        logging.error(f"Archivo de configuración no encontrado: {CITIES_FILE}")
        return []
    try:
        with open(CITIES_FILE, "r", encoding="utf-8") as file:
            return yaml.safe_load(file).get("cities", [])
    except Exception as e:
        logging.error(f"Error leyendo YAML: {e}")
        return []

# 4. API 1: Obtener Datos del Clima (Weather)

def get_weather_data(lat, lon):
    """
    Servicio de Clima Actual
    Obtener temperatura, humedad y viento.
    """
    try:
        # URL API Weather
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"API Weather Fallo: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"API Weather Error de conexión: {e}")
        return None

# 5. API 2: Obtener Datos de Polución (Air Pollution)

def get_pollution_data(lat, lon):
    """
    Servicio de Calidad del Aire
    Obtener PM2.5, PM10 y CO.
    """
    try:
        # URL API Air Pollution
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"API Air Pollution: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"API Air Pollution - Error de conexión: {e}")
        return None

# 6. Función: Guardar en Bronze

def save_to_bronze(data_buffer):
    """Guarda los datos acumulados en un archivo JSON."""
    try:
        timestamp_str = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        filename = f"raw_weather_data_{timestamp_str}.json"
        output_path = BRONZE_FOLDER / filename
        
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data_buffer, file, indent=2)
            
        logging.info(f"Archivo guardado exitosamente: {filename}")
    except Exception as e:
        logging.critical(f"Error de disco: {e}")

# 7. Proceso Principal

def start_ingestion_process():

    logging.info("Iniciando proceso de Ingesta")
    if not API_KEY:
        logging.critical("API Key no definida.")
        return

    cities = load_cities_config()
    raw_data_buffer = []

    for city in cities:
        name = city.get("name")
        lat = city.get("lat")
        lon = city.get("lon")
        
        logging.info(f" Descargando datos de la Ciudad de: {name}")

        # Llamada A: Clima
        weather_result = get_weather_data(lat, lon)

        # Llamada B: Polución
        pollution_result = get_pollution_data(lat, lon)

        # 8. Validación 
        # Solo guardamos si AMBAS llamadas respondieron correctamente

        if weather_result and pollution_result:
            record = {
                "city_metadata": city,
                "ingestion_timestamp": datetime.now().isoformat(),
                "weather_raw_data": weather_result,  # Respuesta de API 1
                "pollution_raw_data": pollution_result # Respuesta de API 2
            }
            raw_data_buffer.append(record)
        else:
            logging.warning(f"Datos incompletos para {name}. Se omite registro.")

    # 9. Persistencia

    if raw_data_buffer:
        save_to_bronze(raw_data_buffer)
    else:
        logging.warning("No se generaron datos para guardar.")

if __name__ == "__main__":
    start_ingestion_process()