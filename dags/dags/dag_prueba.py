from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='openweather_pipeline_simple',
    schedule_interval='*/9 * * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['simple', 'produccion']
) as dag:

    t1_ingest = BashOperator(
        task_id='1_ingesta_bronze',
        bash_command='python /opt/airflow/pipeline/ingestion/openweather_batch_ingest.py'
    )

    t2_transform = BashOperator(
        task_id='2_transform_silver',
        bash_command='python /opt/airflow/pipeline/transform/openweather_batch_transform.py'
    )

    t3_load = BashOperator(
        task_id='3_carga_db',
        bash_command='python /opt/airflow/pipeline/load/openweather_load_database.py'
    )

    t4_report = BashOperator(
        task_id='4_generar_reporte_gold',
        bash_command='python /opt/airflow/pipeline/reports_to_gold/openweather_gold_report.py'
    )

    t1_ingest >> t2_transform >> t3_load >> t4_report