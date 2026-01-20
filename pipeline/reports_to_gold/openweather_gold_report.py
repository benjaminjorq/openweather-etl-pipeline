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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", # Formato limpio para ver bien las tablas
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_gold_reports.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 3. Función: Etiquetar Calidad del Aire

def label_pm25_level(pm25):
    if pd.isna(pm25): 
        return "Desconocido"
    if pm25 < 12: 
        return "Buena"
    elif pm25 <= 35:
        return "Moderada"
    elif pm25 <= 55:
        return "Dañina"
    else: 
        return "Peligrosa"

# 4. Función: Etiquetar Temperatura

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
    
# 5. Función: Etiquetar niveles de CO
    
def label_co_level(co):
    if pd.isna(co): 
        return "Desconocido"
    if co < 4400: 
        return "Buena"
    elif co <= 9400:
        return "Moderada"
    elif co <= 12400:
        return "Dañina"
    else: 
        return "Peligrosa"
    
# 6. Función: Etiquetar niveles de NO2

def label_no2_level(no2):
    if pd.isna(no2): 
        return "Desconocido"
    if no2 < 40: 
        return "Buena"
    elif no2 <= 90:
        return "Moderada"
    elif no2 <= 120:
        return "Dañina"
    else: 
        return "Peligrosa"
    
# 7. Función: Etiquetar niveles de O3

def label_o3_level(o3):
    if pd.isna(o3): 
        return "Desconocido"
    if o3 < 60: 
        return "Buena"
    elif o3 <= 120:
        return "Moderada"
    elif o3 <= 180:
        return "Dañina"
    else: 
        return "Peligrosa"

# 8. Proceso Principal
def create_gold_reports():
    logging.info("Inicio de generación de reportes")
    
    # A. Buscar archivo Silver

    now = datetime.now()
    path = SILVER_FOLDER / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    
    if not path.exists():
        logging.warning("No hay carpeta Silver de hoy")
        return

    files = list(path.glob("clean_weather_data_*.csv"))
    if not files: return
    
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest_file)

    # B. Enriquecimiento

    df["weather_label"] = df["temperature_c"].apply(label_temperature)
    df["pm25_label"] = df["pm2_5_level"].apply(label_pm25_level)
    df["co_label"] = df["co_level"].apply(label_co_level)
    df["no2_label"] = df["no2_level"].apply(label_no2_level)
    df["o3_label"] = df["o3_level"].apply(label_o3_level)
    date_suffix = now.strftime("%Y_%m_%d")

    # Reporte 1: Ranking

    top5_df = df.sort_values(by="pm2_5_level", ascending=False).head(5)

    ranking_cols = [
        "city", "country", 
        "pm2_5_level", "pm25_label", 
        "co_level", "co_label",
        "no2_level", "no2_label",
        "o3_level", "o3_label",
        "temperature_c", "weather_label"
    ]
    
    # Guardar CSV
    ranking_path = RANKING_DIR / f"ranking_pollution_{date_suffix}.csv"
    top5_df[ranking_cols].to_csv(ranking_path, index=False)
    
    # Mostrar tablas en logs
    logging.info(f"\nRanking Top 5 Contaminación\n{top5_df[ranking_cols].to_string(index=False)}")
    logging.info(f"Guardado en: {ranking_path.name}")

    # Reporte 2: Resumen por pais
    summary_df = df.groupby("country")[["temperature_c", "pm2_5_level", "pm10_level","humidity_pct", "co_level", "no2_level", "o3_level"]].mean().reset_index().round(2)
    
    # Guardar CSV
    summary_path = SUMMARY_DIR / f"summary_country_{date_suffix}.csv"
    summary_df.to_csv(summary_path)
    
    # Mostrar tabla en logs
    logging.info(f"\nResumen por País (Vista Previa)\n{summary_df.head().to_string()}")
    logging.info(f"Guardado en: {summary_path.name}")

if __name__ == "__main__":
    create_gold_reports()