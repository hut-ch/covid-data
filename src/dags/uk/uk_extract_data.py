"""Extract Covid source data for UK data sources"""

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator

from extract import process_endpoints
from utils import get_details, get_set_config

with DAG(
    dag_id="extract-uk-data",
    description="Extract UK source data then zip and store in csv format",
    render_template_as_native_obj=True,
) as extract_uk_dag:

    setup_environment = PythonOperator(
        task_id="get-environment-details",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    variables = XComArg(setup_environment)

    get_endpoints = PythonOperator(
        task_id="get-uk-endpoints",
        python_callable=get_details,
        op_kwargs={"location": "uk"},
    )

    endpoints = XComArg(get_endpoints)

    extract = PythonOperator(
        task_id="extract-uk-data",
        python_callable=process_endpoints,
        op_kwargs={"locations": endpoints, "env_vars": variables},
    )

setup_environment >> get_endpoints >> extract
