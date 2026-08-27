
import pandas as pd
import logging

def validate_schema_and_volume(df, required_columns):
    """Valida que el DataFrame no esté vacío y contenga todas las columnas requeridas."""
    if len(df) == 0: 
        logging.critical("El df llegó vacío desde la capa anterior.")
        raise ValueError("El DataFrame no tiene filas para procesar.")

    for column in required_columns:
        if column not in df.columns:
            logging.critical(f"El esquema es inválido. Falta la columna: {column}")
            raise KeyError(f"Falta columna requerida: {column}")

def remove_duplicate_records(df):
    """Elimina registros duplicados exactos basados en ciudad y fecha."""
    return df.drop_duplicates(subset=["city", "processed_timestamp"])

def remove_missing_values(df):
    """Elimina registros nulos críticos (ciudad o temperatura)."""
    return df.dropna(subset=["city", "temperature_c"])

def filter_valid_temperatures(df):
    """Filtra temperaturas dentro del rango físico lógico (-90 a 60)."""
    return df[(df["temperature_c"] > -90) & (df["temperature_c"] < 60)]

def filter_valid_countries(df):
    """Filtra países para que cumplan con el formato de código ISO de 2 letras mayúsculas."""
    valid_format = df["country"].astype(str).str.fullmatch(r"^[A-Z]{2}$", na=False) 
    return df[valid_format]

def apply_data_quality(df):
    """Aplica las reglas de calidad secuencialmente y audita los registros descartados."""
    logging.info("Iniciando validaciones de Calidad de Datos")
    
    rows_before = len(df)
    df = remove_duplicate_records(df)
    if rows_before - len(df) > 0:  
        logging.warning(f"Duplicados Exactos: Se descartaron {rows_before - len(df)} registros.")
    
    rows_before = len(df)
    df = remove_missing_values(df)
    if rows_before - len(df) > 0:  
        logging.warning(f"Faltan datos críticos: Se descartaron {rows_before - len(df)} registros.")
    
    rows_before = len(df)
    df = filter_valid_temperatures(df)
    if rows_before - len(df) > 0:
        logging.warning(f"Temperatura imposible: Se descartaron {rows_before - len(df)} registros.")
    
    rows_before = len(df)
    df = filter_valid_countries(df)
    if rows_before - len(df) > 0:
        logging.warning(f"Código de país mal escrito: Se descartaron {rows_before - len(df)} registros.")

    logging.info("\nResumen Estadístico de los datos validados:\n" + df.describe().to_string())
    
    return df