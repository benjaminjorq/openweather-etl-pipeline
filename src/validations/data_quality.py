"""
Módulo de Calidad de Datos (Data Quality)

Contiene validaciones de esquema, reglas de calidad de datos,
y el orquestador que aplica las reglas de manera secuencial.
"""

import pandas as pd
import logging

# 1. Validaciones de Esquema 

def validate_schema_and_volume(df: pd.DataFrame, required_columns: list) -> None:
    """
    Sanity Check: Valida que el df no esté vacío y contenga todas las columnas esperadas.
    
    Args:
        df (pd.DataFrame): DataFrame original.
        required_columns (list): Lista de columnas que deben existir.
        
    Raises:
        ValueError: Si el DataFrame está vacío.
        KeyError: Si falta alguna columna requerida.
    """
    if len(df) == 0: 
        logging.critical("El df llegó vacío desde la capa anterior.")
        raise ValueError("El DataFrame no tiene filas para procesar.")

    for column in required_columns:
        if column not in df.columns:
            logging.critical(f"El esquema es inválido. Falta la columna: {column}")
            raise KeyError(f"Falta columna requerida: {column}")

# 2. Reglas de Validación Puras

def remove_duplicate_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina registros duplicados basados en ciudad y fecha.
    
    Args:
        df (pd.DataFrame): DataFrame original.
        
    Returns:
        pd.DataFrame: DataFrame sin duplicados exactos.
    """
    df = df.drop_duplicates(subset=["city", "processed_timestamp"])

    return df

def remove_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina registros sin ciudad o temperatura.
    
    Args:
        df (pd.DataFrame): DataFrame original.
        
    Returns:
        pd.DataFrame: DataFrame sin nulos críticos.
    """
    df = df.dropna(subset=["city", "temperature_c"])

    return df

def filter_valid_temperatures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra temperaturas dentro de un rango físico lógico (-90 a 60).
    
    Args:
        df (pd.DataFrame): DataFrame original.
        
    Returns:
        pd.DataFrame: DataFrame con temperaturas válidas.
    """
    df = df[(df["temperature_c"] > -90) & (df["temperature_c"] < 60)]

    return df

def filter_valid_countries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra países para que cumplan con el código ISO de 2 letras mayúsculas.
    
    Args:
        df (pd.DataFrame): DataFrame original.
        
    Returns:
        pd.DataFrame: DataFrame con códigos de país válidos.
    """
    valid_format = df["country"].astype(str).str.fullmatch(r"^[A-Z]{2}$", na=False) 
    df = df[valid_format]

    return df

# 3. Orquestador de Calidad

def apply_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica secuencialmente las reglas de calidad de datos y audita los registros descartados.
    
    Args:
        df (pd.DataFrame): DataFrame original.
        
    Returns:
        pd.DataFrame: DataFrame final tras pasar las reglas de negocio.
    """
    logging.info("Iniciando validaciones de Calidad de Datos")
    
    # 1. Duplicados Exactos

    rows_before = len(df)
    df = remove_duplicate_records(df)
    if rows_before - len(df) > 0:  
        logging.warning(f"Duplicados Exactos: Se descartaron {rows_before - len(df)} registros.")
    
    # 2. Faltan datos críticos

    rows_before = len(df)
    df = remove_missing_values(df)
    if rows_before - len(df) > 0:  
        logging.warning(f"Faltan datos críticos: Se descartaron {rows_before - len(df)} registros.")
    
    # 3. Filtros de temperatura

    rows_before = len(df)
    df = filter_valid_temperatures(df)
    if rows_before - len(df) > 0:
        logging.warning(f"Temperatura imposible: Se descartaron {rows_before - len(df)} registros.")
    
    # 4. Código de país mal escrito

    rows_before = len(df)
    df = filter_valid_countries(df)
    if rows_before - len(df) > 0:
        logging.warning(f"Código de país mal escrito: Se descartaron {rows_before - len(df)} registros.")

    logging.info("\nResumen Estadístico de los datos validados:\n" + df.describe().to_string())
    
    return df