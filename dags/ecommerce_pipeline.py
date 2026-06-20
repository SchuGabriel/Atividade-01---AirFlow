from airflow.sdk import dag, task
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pendulum
import requests
import logging

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "on_success_callback": on_success_callback,
    "on_retry_callback": on_retry_callback,
    "on_failure_callback": on_failure_callback,
}

@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "pipeline"]
)

def ecommerce_pipeline():

    @task
    def fetch_products():
        try:
            url = "https://fakestoreapi.com/products"
            response = requests.get(url, timeout=10)

            response.raise_for_status()

            data = response.json()

            logging.info(f"{len(data)} produtos encontrados")

            return data

        except Exception as e:
            logging.error(f"Erro ao buscar produtos: {e}")
            raise

    fetch_products()

def on_success_callback(context):
    logging.info("Task executada com sucesso")

def on_retry_callback(context):
    logging.warning("Task em retry")

def on_failure_callback(context):
    logging.error("Task falhou")


ecommerce_pipeline()