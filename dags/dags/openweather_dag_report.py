from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    'owner': 'benjamin_jorquera',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='openweather_daily_report',
    default_args=default_args,
    description='Genera el reporte y ranking diario de la capa Gold',
    schedule_interval='50 23 * * *', 
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=['OpenWeather Gold'],
) as dag:

    # Tarea: Generar reporte consolidado de la información reunida durante el dia

    t1_daily_report = BashOperator(
        task_id='generate_daily_gold_report',
        bash_command='python /opt/airflow/src/reports_to_gold/openweather_gold_report.py'
    )