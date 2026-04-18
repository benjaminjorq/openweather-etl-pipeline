from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.ingestion.openweather_batch_ingest import start_ingestion_process
from src.transform.openweather_batch_transform import start_transformation_process
from src.load.openweather_load_database import start_database_load


# 1. Definición de Argumentos por Defecto 

default_args = {
    'owner': 'benjamin_jorquera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
}

# 2. DAG

with DAG(
    dag_id='openweather_pipeline_ETL',
    default_args=default_args,
    description='ETL Clima Chile: Arquitectura Medallion con carga a PostgreSQL',
    schedule_interval='0 * * * *', 
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"), # Hora local de Chile
    catchup=False,
    tags=['OpenWeather Extract-Transform-Load'],
) as dag:

# 3. Definición de Tareas 
    
    # 3.1 Ingesta: Extrae datos crudos de la API

    t1_ingest = PythonOperator(
        task_id='1_ingest_bronze_data',
        python_callable=start_ingestion_process
    )

    # 3.2 Transformación: Limpia los datos con Pandas

    t2_transform = PythonOperator(
        task_id='2_transform_to_silver',
        python_callable=start_transformation_process
    )

    # 3.3 Carga: Inserta los datos limpios en PostgreSQL

    t3_load = PythonOperator(
        task_id='3_load_to_database',
        python_callable=start_database_load
    )

# 4. Configuración de Dependencias
    
    t1_ingest >> t2_transform >> t3_load 
