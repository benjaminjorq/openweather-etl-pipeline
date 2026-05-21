from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.reports_to_gold.openweather_gold_report import create_gold_reports
from src.utils.alerts import send_discord_failure_alert


# 1. Definición de Argumentos por Defecto 

default_args = {
    'owner': 'benjamin_jorquera',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_discord_failure_alert,
}

# 2. DAG

with DAG(
    dag_id='openweather_daily_report',
    default_args=default_args,
    description='Generación de Ranking y Reportes (Capa Gold)',
    schedule_interval='50 23 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=['OpenWeather Gold'],
) as dag:

# 3. Definición de Tarea

    # 3.1 Reportes: Genera Ranking y Promedios por Pais

    t1_daily_report = PythonOperator(
        task_id='generate_daily_gold_report',
        python_callable=create_gold_reports,
        op_kwargs={'execution_date_short': '{{ ds_nodash }}'}
    )