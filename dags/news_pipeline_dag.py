from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'news_pipeline',
    default_args=default_args,
    description='Pipeline Batch: Scraping -> Silver (Clean) -> Gold (Aggregations)',
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    scrape_task = BashOperator(
        task_id='scrape_articles',
        bash_command='python /opt/airflow/scrapers/batch_scraper.py'
    )

    clean_task = BashOperator(
        task_id='clean_silver',
        bash_command='python /opt/airflow/jobs/silver_cleaning.py'
    )

    aggregate_task = BashOperator(
        task_id='aggregate_gold',
        bash_command='python /opt/airflow/jobs/gold_aggregations.py'
    )

    scrape_task >> clean_task >> aggregate_task
