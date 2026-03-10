import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# 1. Configuración de Rutas

BASE_DIR = Path("/opt/airflow")
LOG_DIR = BASE_DIR / "logs"
SILVER_FOLDER = BASE_DIR / "data/silver"
GOLD_FOLDER = BASE_DIR / "data/gold"

RANKING_DIR = GOLD_FOLDER / "ranking"
SUMMARY_DIR = GOLD_FOLDER / "summary"

# 2. Creación de carpetas

LOG_DIR.mkdir(parents=True, exist_ok=True)
GOLD_FOLDER.mkdir(parents=True, exist_ok=True)
RANKING_DIR.mkdir(parents=True, exist_ok=True)
SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

# 3. Configuración de Logs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", # Formato limpio para ver bien las tablas
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_gold_reports.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 4. Función: Etiquetar Calidad del Aire

def label_pm25_level(pm25):
    if pd.isna(pm25): 
        return "Desconocido"
    if pm25 < 12: 
        return "Bueno"
    elif pm25 <= 35:
        return "Moderado"
    elif pm25 <= 55:
        return "Dañino"
    else: 
        return "Peligroso"

# 5. Función: Etiquetar Temperatura

def label_temperature(temp):
    if pd.isna(temp):
        return "Desconocido"
    if temp < 10: 
        return "Frío"
    elif temp < 20: 
        return "Fresco"
    elif temp < 30:
        return "Agradable"
    else: 
        return "Caluroso"
    
# 6. Función: Etiquetar niveles de CO
    
def label_co_level(co):
    if pd.isna(co): 
        return "Desconocido"
    if co < 4400: 
        return "Bueno"
    elif co <= 9400:
        return "Moderado"
    elif co <= 12400:
        return "Dañina"
    else: 
        return "Peligrosa"
    
# 7. Función: Etiquetar niveles de NO2

def label_no2_level(no2):
    if pd.isna(no2): 
        return "Desconocido"
    if no2 < 40: 
        return "Bueno"
    elif no2 <= 90:
        return "Moderado"
    elif no2 <= 120:
        return "Dañina"
    else: 
        return "Peligrosa"
    
# 8. Función: Etiquetar niveles de O3

def label_o3_level(o3):
    if pd.isna(o3): 
        return "Desconocido"
    if o3 < 60: 
        return "Bueno"
    elif o3 <= 120:
        return "Moderado"
    elif o3 <= 180:
        return "Dañino"
    else: 
        return "Peligroso"
    
# 9. Función Etiquetar niveles de PM 10

def label_pm10_level(pm10):
    if pd.isna(pm10):
        return "Desconocido"
    if pm10 < 54:
        return "Bueno"
    elif pm10 <= 154: 
        return "Moderado"
    else:
        return "Malo"
    
# 10. Función Etiquetar niveles de Humedad

def label_humidity(hum):
    if pd.isna(hum): 
        return "Desc."
    if hum < 30: 
        return "Seco"
    elif hum < 70: 
        return "Ideal"
    else:
        return "Humedo"

# 11. Proceso Principal

def create_gold_reports():

    logging.info("Inicio de generación de reportes")
    
    # A. Buscar archivos de la capa Silver

    now = datetime.now()
    daily_silver_path = SILVER_FOLDER / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    
    if not daily_silver_path.exists():
        logging.warning("No se encontró la carpeta Silver de hoy.")
        return

    daily_files = list(daily_silver_path.glob("clean_weather_data_*.csv")) 
    if not daily_files: 
        logging.warning("No se generaron archivos CSV hoy.")
        return
    
    # B. Consolidar y promediar los datos diarios

    dataframes_list = [pd.read_csv(file) for file in daily_files]
    raw_df = pd.concat(dataframes_list, ignore_index=True)

    logging.info(f"Consolidación exitosa. Total de registros: {len(raw_df)}")

    all_numeric_cols = ["temperature_c", "aqi", "pm2_5_level", "pm10_level", "co_level", "no2_level", "o3_level", "humidity_pct"]
    df = raw_df.groupby(["city", "country"])[all_numeric_cols].mean().reset_index().round(1)

    logging.info(f"Registros únicos tras calcular el promedio diario: {len(df)}")

    # C. Enriquecimiento de Datos (Aplicación de etiquetas)

    df["weather_label"] = df["temperature_c"].apply(label_temperature)
    df["pm25_label"] = df["pm2_5_level"].apply(label_pm25_level)
    df["co_label"] = df["co_level"].apply(label_co_level)
    df["no2_label"] = df["no2_level"].apply(label_no2_level)
    df["o3_label"] = df["o3_level"].apply(label_o3_level)
    df["hum_label"] = df["humidity_pct"].apply(label_humidity)

    # C. Particiones por fecha para los archivos de salida

    date_suffix = now.strftime("%Y_%m_%d")

    # REPORTE 1 : Ranking

    top7_df = df.sort_values(by=["aqi", "pm2_5_level"], ascending=False).head(7)

    # A. Definir columnas a guardar (Nombre y Etiqueta)

    ranking_cols = ["city", "country", "aqi", "pm2_5_level", "pm25_label", "co_level",
                    "co_label", "no2_level", "no2_label", "o3_level", "o3_label", "temperature_c", "weather_label"]

    # B. Definir vista para Logs 

    ranking_views = {
        "city": "Ciudad", "country": "Pais", "aqi": "AQI",
        "pm2_5_level": "PM2.5", "pm25_label": "Est. PM2.5", 
        "co_level": "CO", "co_label": "Est. CO",
        "no2_level": "NO2", "no2_label": "Est. NO2",
        "o3_level": "O3", "o3_label": "Est. O3",
        "temperature_c": "Temp", "weather_label": "Clima"
    }

    # C. Guardar CSV

    ranking_path = RANKING_DIR / f"ranking_pollution_{date_suffix}.csv"
    top7_df[ranking_cols].to_csv(ranking_path, index=False)
    
    # D. Imprimir en Logs

    logging.info("Ranking Top 7 Contaminación")
    ranking_df = top7_df[list(ranking_views.keys())].rename(columns=ranking_views)
    logging.info("\n" + ranking_df.to_string(index=False, justify='left'))
    logging.info(f"\nGuardado en: {ranking_path.name}")

    # REPORTE 2 : Resumen Promedio por Pais
    
    # A. Calcular Promedios Numéricos

    num_cols = ["temperature_c", "pm2_5_level", "pm10_level","humidity_pct", "co_level", "no2_level"]
    summary_df = df.groupby("country")[num_cols].mean().reset_index().round(1)

    # B. Aplicación de etiquetas para promedios

    summary_df["weather_label"] = summary_df["temperature_c"].apply(label_temperature)
    summary_df["hum_label"] = summary_df["humidity_pct"].apply(label_humidity)
    summary_df["pm25_label"] = summary_df["pm2_5_level"].apply(label_pm25_level)
    summary_df["pm10_label"] = summary_df["pm10_level"].apply(label_pm10_level)
    summary_df["co_label"] = summary_df["co_level"].apply(label_co_level)
    summary_df["no2_label"] = summary_df["no2_level"].apply(label_no2_level)

    # C. Definir vista para Logs

    summary_views = {
        "country": "Pais",
        "temperature_c": "Temp", "weather_label": "Clima",
        "humidity_pct": "Hum %", "hum_label": "Est. Hum",
        "pm2_5_level": "PM2.5", "pm25_label": "Est. PM2.5",
        "pm10_level": "PM10", "pm10_label": "Est. PM10",
        "co_level": "CO", "co_label": "Est. CO",
        "no2_level": "NO2", "no2_label": "Est. NO2"
    }
    
    # D. Guardar CSV

    summary_path = SUMMARY_DIR / f"summary_country_{date_suffix}.csv"
    summary_df.to_csv(summary_path, index=False)
    
    # E. Imprimir en Logs

    logging.info(" Resumen Promedio por Pais (Vista Previa): ")
    view_summary = summary_df[list(summary_views.keys())].rename(columns=summary_views)
    logging.info("\n" + view_summary.to_string(index=False, justify='left'))
    logging.info(f"\nGuardado en: {summary_path.name}")

# 12. Ejecución del Script

if __name__ == "__main__":
    create_gold_reports()