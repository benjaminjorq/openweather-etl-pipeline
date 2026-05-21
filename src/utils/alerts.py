import os
import requests
import logging


def send_discord_failure_alert(context: dict) -> None:
    """
    Envía una alerta de fallo a Discord mediante un webhook.

    Extrae información del contexto de ejecución de Airflow, incluyendo
    el DAG, la tarea y la fecha de ejecución, para construir y enviar
    una notificación de error a un canal de Discord configurado mediante
    la variable de entorno `DISCORD_WEBHOOK_URL`.

    La función maneja internamente errores de conexión y ejecución para
    evitar afectar el flujo principal del pipeline.

    Args:
        context (dict): Diccionario de contexto entregado por Airflow.

    Returns:
        None
    """

    # Validar que la URL del webhook exista antes de continuar

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logging.warning("La variable DISCORD_WEBHOOK_URL no está configurada. Alerta cancelada.")
        return

    try:
        # 1. Extraer las variables de contexto del fallo en Airflow

        task_instance = context.get('task_instance')
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        execution_date = str(context.get('logical_date', context.get('execution_date')))
        log_url = task_instance.log_url

        # 2. Mensaje de alerta al canal de Discord

        alert_message = (
            "**Pipeline Execution Failure**\n"
            f"**DAG ID:** `{dag_id}`\n"
            f"**Task ID:** `{task_id}`\n"
            f"**Execution Date:** `{execution_date}`\n"
            f"**Logs:** {log_url}"
        )

        # 3. Construir los datos de la alerta

        alert_data = {
            "content": alert_message,
            "username": "Airflow Monitor"
        }

        # 4. Enviar la petición HTTP POST al webhook

        response = requests.post(webhook_url, json=alert_data, timeout=10)
        response.raise_for_status()
        logging.info("Alerta de Discord enviada con éxito.")

    except requests.exceptions.RequestException as req_error:
        # Capturar errores específicos de conexión (ej. si Discord está caído)
        logging.error(f"Falló la petición HTTP al Webhook de Discord: {req_error}")
        
    except Exception as e:
        # Capturar cualquier otro error inesperado en la ejecución
        logging.error(f"Error inesperado al procesar la alerta de Discord: {e}")