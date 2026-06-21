from airflow.sdk import dag, task
from datetime import timedelta
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pendulum
import requests
import logging

def on_success_callback(context):
    logging.info("Task executada com sucesso")


def on_retry_callback(context):
    logging.warning("Task em retry")


def on_failure_callback(context):
    logging.error("Task falhou")


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
    start_date=pendulum.datetime(
        2024, 1, 1, tz="America/Sao_Paulo"
    ),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "pipeline"],
)

def ecommerce_pipeline():

    @task
    def fetch_products():
        try:
            response = requests.get(
                "https://fakestoreapi.com/products",
                timeout=10,
            )

            response.raise_for_status()

            data = response.json()

            logging.info(
                f"{len(data)} produtos encontrados"
            )

            return data

        except Exception as e:
            logging.error(
                f"Erro ao buscar produtos: {e}"
            )
            raise

    @task
    def extract_categories(products):
        categories = list(
            set(product["category"] for product in products)
        )
        logging.info(
            f"Categorias encontradas: {categories}"
        )

        return categories

    @task(pool="ecommerce_pool")
    def calculate_metrics(category, products):
        category_products = [
            product
            for product in products
            if product["category"] == category
        ]
        prices = [
            product["price"]
            for product in category_products
        ]
        result = {
            "category": category,
            "avg_price": round(sum(prices) / len(prices), 2),
            "min_price": min(prices),
            "max_price": max(prices),
            "product_count": len(category_products),
        }
        logging.info(f"Métricas calculadas: {result}")
        return result

    @task
    def consolidate_metrics(metrics):
        metrics_list = list(metrics)
        logging.info(
            f"{len(metrics_list)} categorias processadas"
        )
        for metric in metrics_list:
            logging.info(metric)

        return metrics_list 

    @task
    def save_to_postgres(metrics):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        execution_date = datetime.now().date()

        for item in metrics:
            cursor.execute(
                """
                INSERT INTO category_metrics (
                    category,
                    avg_price,
                    min_price,
                    max_price,
                    product_count,
                    execution_date
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, execution_date)
                DO NOTHING
                """,
                (
                    item["category"],
                    item["avg_price"],
                    item["min_price"],
                    item["max_price"],
                    item["product_count"],
                    execution_date,
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Dados salvos no PostgreSQL com sucesso")

    with TaskGroup(group_id="ingestao"):
        products = fetch_products()
    
        categories = extract_categories(products)

    with TaskGroup(group_id="analise"):
    
        metrics = calculate_metrics.partial(
            products=products
        ).expand(
            category=categories
        )
    
        clean_metrics = consolidate_metrics(metrics)
        save_to_postgres(clean_metrics)

ecommerce_pipeline()