
import requests
import json
import logging
import yaml
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path


load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

BASE_DIR = Path("/opt/airflow")
LOG_DIR = BASE_DIR / "logs"
BRONZE_FOLDER = BASE_DIR / "data/bronze"
CITIES_FILE = BASE_DIR / "config/cities.yaml"

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

def load_cities_config():
    """
    Lee el archivo de configuración YAML de las ciudades a procesar.

    Returns:
        list[dict]: Lista de diccionarios con las claves 'name', 'lat' y 'lon'.

    Raises:
        FileNotFoundError: Si el archivo cities.yaml no existe.
        RuntimeError: Si ocurre un error al leer el archivo.
    """

    if not CITIES_FILE.exists():
        logging.error(f"Archivo de configuración no encontrado: {CITIES_FILE}")
        raise FileNotFoundError(f"No se encontró el archivo: {CITIES_FILE}")
    
    try:
        with open(CITIES_FILE, "r", encoding="utf-8") as file:
            return yaml.safe_load(file).get("cities", [])
        
    except Exception as e:
        logging.error(f"Error leyendo YAML: {e}")
        raise RuntimeError("Fallo procesando archivo YAML") from e


def get_weather_data(lat, lon):
    """
    Obtiene los datos meteorológicos para unas coordenadas definidas.

    Args:
        lat (float): Latitud de la ciudad.
        lon (float): Longitud de la ciudad.

    Returns:
        dict | None: JSON con los datos del Clima. Retorna None si falla.
    """
    try:
        
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"API Weather Fallo: {response.status_code}")
            return None
        
    except Exception as e:
        logging.error(f"API Weather Error de conexión: {e}")
        return None


def get_pollution_data(lat, lon):
    """
    Obtiene los datos de contaminación atmosférica para unas coordenadas definidas.

    Args:
        lat (float): Latitud de la ciudad.
        lon (float): Longitud de la ciudad.

    Returns:
        dict | None: JSON con los datos de polución. Retorna None si falla.
    """
    try:
        
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"API Air Pollution: {response.status_code}")
            return None
        
    except Exception as e:
        logging.error(f"API Air Pollution - Error de conexión: {e}")
        return None


def start_ingestion_process(execution_date):
    """
    Ejecuta el proceso de ingesta de datos desde APIs externas hacia la capa Bronze.

    Args:
        execution_date (str): Fecha de ejecución lógica en formato 'YYYYMMDDTHHMMSS'.

    Raises:
        RuntimeError: Si la variable de entorno OPENWEATHER_API_KEY no está configurada o no existe.
    """

    logging.info("Iniciando proceso de Ingesta")

    if not API_KEY:
        logging.critical("API Key no definida.")
        raise RuntimeError("Falta OPENWEATHER_API_KEY")

    cities = load_cities_config()
    raw_data_buffer = []

    for city in cities:
        name = city.get("name")
        lat = city.get("lat")
        lon = city.get("lon")
        
        logging.info(f"Descargando datos de la Ciudad de: {name}")

        weather_result = get_weather_data(lat, lon)
        pollution_result = get_pollution_data(lat, lon)

        # Se permite guardar registros parciales si al menos una API responde.
        # Esto evita pérdida de datos ante fallos parciales, pero puede generar registros incompletos.

        if weather_result or pollution_result:
            record = {
                "city_metadata": city,
                "ingestion_timestamp": execution_date,
                "weather_raw_data": weather_result,  
                "pollution_raw_data": pollution_result 
            }
            raw_data_buffer.append(record)
        else:
            logging.warning(f"Datos incompletos para {name}. Se omite registro.")

    if raw_data_buffer:
        filename = f"raw_weather_data_{execution_date}.json"
        output_path = BRONZE_FOLDER / filename
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(raw_data_buffer, file, indent=2)

        logging.info(f"Archivo guardado en Bronze: {filename}")

if __name__ == "__main__":

    test_date = datetime.now().strftime('%Y%m%dT%H%M%S')
    start_ingestion_process(execution_date=test_date)