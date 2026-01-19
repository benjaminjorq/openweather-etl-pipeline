from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- PASO 1: LA FUNCIÓN ---
def guardar_hora():
    hora_actual = datetime.now()
    # Cambié el mensaje para que veas que es el automático
    mensaje = f"🤖 Ejecución automática: {hora_actual}\n"
    
    with open('/opt/airflow/data/mi_historial.txt', 'a') as archivo:
        archivo.write(mensaje)
    
    print("¡Listo! Hora guardada.")

# --- PASO 2: EL DAG ---
with DAG(
    dag_id='dag_automatico_1min',    # Le cambié el nombre
    start_date=datetime(2023, 1, 1), # Fecha de inicio en el pasado (necesario)
    schedule='* * * * *',            # <--- ¡LA MAGIA! (Significa: Cada minuto)
    catchup=False                    # Importante: Para que no intente recuperar el tiempo perdido desde 2023
) as dag:

    tarea_guardar = PythonOperator(
        task_id='guardar_dato_auto',
        python_callable=guardar_hora
    )