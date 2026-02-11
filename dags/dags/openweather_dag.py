from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# 1. Definición de Argumentos por Defecto 

default_args = {
    'owner': 'benjamin_jorquera',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # Si falla, Airflow lo intentará una vez más automáticamente
    'retry_delay': timedelta(minutes=5), # Espera de 5 minutos antes del reintento
}

# 2. DAG

with DAG(
    dag_id='openweather_pipeline_simple',
    default_args=default_args,
    description='ETL Clima Chile: Arquitectura Medallion con carga a PostgreSQL',
    schedule_interval='0 * * * *', # Se ejecuta una vez por hora (minuto 0)
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"), # Hora local de Chile
    catchup=False,
    tags=['OpenWeather Medallion Pipeline'],
) as dag:

# 3. Definición de Tareas (Capa Bronze, Silver y Gold)
    
    # Ingesta: Extrae datos crudos de la API

    t1_ingest = BashOperator(
        task_id='1_ingesta_bronze',
        bash_command='python /opt/airflow/pipeline/ingestion/openweather_batch_ingest.py'
    )

    # Transformación: Limpia los datos con Pandas

    t2_transform = BashOperator(
        task_id='2_transform_silver',
        bash_command='python /opt/airflow/pipeline/transform/openweather_batch_transform.py'
    )

    # Carga: Inserta los datos limpios en PostgreSQL

    t3_load = BashOperator(
        task_id='3_carga_db',
        bash_command='python /opt/airflow/pipeline/load/openweather_load_database.py'
    )

    # Reporte: Genera el ranking de contaminación 
    t4_report = BashOperator(
        task_id='4_generar_reporte_gold',
        bash_command='python /opt/airflow/pipeline/reports_to_gold/openweather_gold_report.py'
    )

    # 4. Configuración de Dependencias
    
    t1_ingest >> t2_transform >> t3_load >> t4_report