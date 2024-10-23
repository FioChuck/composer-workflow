from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from pyfiglet import Figlet

args = {
    'owner': 'packt-developer',
}

query = f"""
    CREATE OR REPLACE TABLE
    `cf-data-analytics.market_data.googl_daily_bar` AS
    WITH
    daily_bar AS (
    SELECT
        symbol,
        prevDailyBar.c AS price,
        EXTRACT(DATE
        FROM
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', prevdailyBar.t)) current_dt
    FROM
        `cf-data-analytics.market_data.googl`)
    SELECT
    symbol,
    current_dt as dt,
    ARRAY_AGG(price
    ORDER BY
        price DESC) [
    OFFSET
    (0)] AS closing_price,
    FROM
    daily_bar
    GROUP BY
    symbol,
    dt
    ORDER BY current_dt DESC;
"""


def generate_figlet_text():
    f = Figlet(font='slant')
    print("\n" + f.renderText('HELLO DR. HALEY'))


with DAG(
    dag_id='gcs-bq',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    is_paused_upon_creation=False

) as dag:

    ctas_query = BigQueryInsertJobOperator(
        task_id="aggregation_query",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False
            }
        }
    ),

    python_task = PythonOperator(
        task_id='generate_figlet',
        python_callable=generate_figlet_text,
    )

    python_task >> ctas_query

if __name__ == "__main__":
    dag.cli()
