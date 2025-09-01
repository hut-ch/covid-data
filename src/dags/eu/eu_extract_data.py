"""Extract Covid source data for EU data sources"""

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator

from extract import process_endpoints
from utils import get_details, get_set_config

with DAG(
    dag_id="extract-eu-data",
    description="Extract EU source data and store in json format",
    render_template_as_native_obj=True,
) as extract_eu_dag:

    setup_environment = PythonOperator(
        task_id="get-environment-details",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    variables = XComArg(setup_environment)

    get_endpoints = PythonOperator(
        task_id="get-eu-endpoints",
        python_callable=get_details,
        op_kwargs={"location": "eu"},
    )

    endpoints = XComArg(get_endpoints)

    extract = PythonOperator(
        task_id="extract-eu-data",
        python_callable=process_endpoints,
        op_kwargs={"locations": endpoints, "env_vars": variables},
    )

setup_environment >> get_endpoints >> extract
