import os
import requests
import logging


def send_discord_failure_alert(context):
    """Envía una alerta de fallo a Discord mediante un webhook."""

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logging.warning("La variable DISCORD_WEBHOOK_URL no está configurada. Alerta cancelada.")
        return
    
    try:
        task_instance = context.get('task_instance')
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        execution_date = str(context.get('logical_date', context.get('execution_date')))
        log_url = task_instance.log_url

        alert_message = (
            "**Pipeline Execution Failure**\n"
            f"**DAG ID:** `{dag_id}`\n"
            f"**Task ID:** `{task_id}`\n"
            f"**Execution Date:** `{execution_date}`\n"
            f"**Logs:** {log_url}"
        )

        alert_data = {
            "content": alert_message,
            "username": "Airflow Monitor"
        }

        response = requests.post(webhook_url, json=alert_data, timeout=10)
        response.raise_for_status()
        logging.info("Alerta de Discord enviada con éxito.")

    except requests.exceptions.RequestException as req_error:
        logging.error(f"Falló la petición HTTP al Webhook de Discord: {req_error}")
        
    except Exception as e:
        logging.error(f"Error inesperado al procesar la alerta de Discord: {e}")