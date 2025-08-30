"""test DAG"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from run_covid_pipeline import process_eu_data, process_uk_data
from utils import get_set_config

with DAG(dag_id="covid_pipeline_full") as main_dag:

    setup_environment = PythonOperator(
        task_id="load_env_file",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    run_eu = PythonOperator(task_id="eu-data", python_callable=process_eu_data)

    run_uk = PythonOperator(task_id="uk-data", python_callable=process_uk_data)

setup_environment >> run_eu >> run_uk
