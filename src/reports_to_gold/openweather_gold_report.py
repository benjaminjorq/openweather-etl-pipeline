
"""
Módulo de Reportes (Gold Layer) - OpenWeather ETL.

Consolida las ejecuciones diarias desde la capa Silver y genera reportes
analíticos exportados como CSV hacia la capa Gold:
  - Ranking Top 7 ciudades más contaminadas del día.
  - Resumen de promedios diarios agrupados por país.

"""

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
    format="%(asctime)s [%(levelname)s] %(message)s", 
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_gold_reports.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 4. Función: Indicador de Negocio

def get_overall_status(aqi_mean):
    """
    Clasifica el estado de calidad del aire a partir del promedio del índice AQI.

    Escala basada en el índice AQI de OpenWeather (1 a 5):
    1 = Excelente, 2 = Bueno, 3 = Moderado, 4 = Malo, 5 = Peligroso.

    Args:
        aqi_mean (float): Valor promedio del índice de calidad del aire.

    Returns:
        str: Categoría de calidad del aire.
    """
    if pd.isna(aqi_mean): 
        return "Desconocido"
    if aqi_mean <= 1.5: 
        return "Excelente"
    elif aqi_mean <= 2.5: 
        return "Bueno"
    elif aqi_mean <= 3.5:
        return "Moderado"
    elif aqi_mean <= 4.5: 
        return "Malo"
    else: 
        return "Peligroso"

# 5. Proceso Principal

def create_gold_reports(execution_date_short: str):
    """
    Consolida las ejecuciones del día y genera los reportes analíticos Gold.

    Lee todos los CSV del día desde Silver, calcula promedios diarios por ciudad
    y país, clasifica la calidad del aire y exporta dos reportes:
      - Ranking Top 7 ciudades más contaminadas.
      - Resumen de promedios por país.

    Args:
        execution_date_short (str): Fecha del día a procesar en formato YYYYMMDD.
    """

    logging.info(f"Generando reportes para el día: {execution_date_short}")
    
    target_dt = datetime.strptime(execution_date_short, "%Y%m%d")
    daily_silver_path = SILVER_FOLDER / f"year={target_dt.year}" / f"month={target_dt.month:02d}" / f"day={target_dt.day:02d}"
    
    if not daily_silver_path.exists():
        logging.warning("No hay datos en Silver para esta fecha.")
        return
        
    daily_files = list(daily_silver_path.glob("clean_weather_data_*.csv"))
    if not daily_files:
        logging.warning("La carpeta existe pero está vacía.")
        return

    # Consolidar todos los CSV del día en un único DataFrame

    dataframes = [pd.read_csv(file) for file in daily_files]
    raw_df = pd.concat(dataframes, ignore_index=True)

    logging.info(f"Consolidación exitosa. Total de registros: {len(raw_df)}")

    numeric_cols = [
        "temperature_c", "humidity_pct", "wind_speed_ms", "pressure_hpa", 
        "aqi", "pm2_5_level", "pm10_level", "co_level", "no2_level", "o3_level"
    ]
    
    # Calcular promedio diario por ciudad y país

    df = raw_df.groupby(["city", "country"])[numeric_cols].mean().reset_index().round(1)
    logging.info(f"Registros únicos tras calcular el promedio diario: {len(df)}")

    # Clasificar calidad del aire y ordenar de más a menos contaminado

    df["overall_status"] = df["aqi"].apply(get_overall_status)
    df = df.sort_values(by=["aqi", "pm2_5_level"], ascending=False)

    # C. Particiones por fecha para los archivos de salida

    date_suffix = target_dt.strftime("%Y_%m_%d")

    # REPORTE 1: Ranking Top 7 Ciudades más Contaminadas

    top7_df = df.head(7)

    # A. Columnas a exportar en el CSV

    ranking_cols = [
        "city", "country", "temperature_c", "humidity_pct", "wind_speed_ms", "pressure_hpa", 
        "aqi", "pm2_5_level", "pm10_level", "co_level", "no2_level", "o3_level", "overall_status"
    ]

    # B. Mapeo de columnas para visualización en logs

    ranking_views = {
        "city": "Ciudad", "country": "Pais",
        "temperature_c": "Temp", "humidity_pct": "Hum %",
        "wind_speed_ms": "Viento", "pressure_hpa": "Presion",
        "aqi": "AQI", "pm2_5_level": "PM2.5", "pm10_level": "PM10", 
        "co_level": "CO", "no2_level": "NO2", "o3_level": "O3", 
        "overall_status": "Estado"
    }

    # C. Guardar CSV en la capa Gold

    ranking_path = RANKING_DIR / f"ranking_pollution_{date_suffix}.csv"
    top7_df[ranking_cols].to_csv(ranking_path, index=False)
    
    # D. Imprimir tabla en logs

    logging.info("Ranking Top 7 Contaminación")
    ranking_df = top7_df[list(ranking_views.keys())].rename(columns=ranking_views)
    logging.info("\n" + ranking_df.to_string(index=False, justify='left'))
    logging.info(f"\nGuardado en: {ranking_path.name}")

    # REPORTE 2: Resumen de Promedios por País
    
    # A. Calcular promedios numéricos agrupados por país

    summary_df = df.groupby("country")[numeric_cols].mean().reset_index().round(1)

    # B. Clasificar calidad del aire y ordenar de más a menos contaminado

    summary_df["overall_status"] = summary_df["aqi"].apply(get_overall_status)
    summary_df = summary_df.sort_values(by="aqi", ascending=False)

    # C. Mapeo de columnas para visualización en logs

    summary_views = {
        "country": "Pais",
        "temperature_c": "Temp", "humidity_pct": "Hum %",
        "wind_speed_ms": "Viento", "pressure_hpa": "Presion",
        "aqi": "AQI", "pm2_5_level": "PM2.5", "pm10_level": "PM10", 
        "co_level": "CO", "no2_level": "NO2", "o3_level": "O3", 
        "overall_status": "Estado"
    }
    
    # D. Guardar CSV en la capa Gold

    summary_path = SUMMARY_DIR / f"summary_country_{date_suffix}.csv"
    summary_df.to_csv(summary_path, index=False)
    
    # E. Imprimir tabla en logs

    logging.info(" Resumen Promedio por Pais (Vista Previa): ")
    view_summary = summary_df[list(summary_views.keys())].rename(columns=summary_views)
    logging.info("\n" + view_summary.to_string(index=False, justify='left'))
    logging.info(f"\nGuardado en: {summary_path.name}")

# 6. Ejecución 

if __name__ == "__main__":
        
        # Prueba manual: Ejecuta el reporte directamente sin Airflow.
        # Consolida todos los CSV del día actual y genera los reportes Gold.

        test_date = datetime.now().strftime('%Y%m%d')
        create_gold_reports(execution_date_short=test_date)

    