"""Run full Covid pipeline"""

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from extract import process_endpoints
from load import (
    create_dimensional_model_eu,
    create_schema,
    maintain_eu_dims,
    maintain_eu_facts,
    maintain_shared_dims,
    maintain_uk_dims,
    maintain_uk_facts,
)
from transform import mi_transform, nd_transform, uk_placeholder, vt_transform
from utils import get_details, get_set_config

with DAG(
    dag_id="covid-pipeline-all",
    description="Run the full Covid ETL pipeline for all sources",
    render_template_as_native_obj=True,
) as run_all_pipeline_dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    eu_transform_complete = EmptyOperator(task_id="eu-transform-end")
    uk_transform_complete = EmptyOperator(task_id="uk-transform-end")
    eu_dims_complete = EmptyOperator(task_id="eu-dimensions-end")
    uk_dims_complete = EmptyOperator(task_id="uk-dimensions-end")
    facts_complete = EmptyOperator(task_id="fact-tables-end")

    setup_environment = PythonOperator(
        task_id="get-environment-details",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    variables = XComArg(setup_environment)

    get_eu_endpoints = PythonOperator(
        task_id="get-eu-endpoints",
        python_callable=get_details,
        op_kwargs={"location": "eu"},
    )

    eu_endpoints = XComArg(get_eu_endpoints)

    get_uk_endpoints = PythonOperator(
        task_id="get-uk-endpoints",
        python_callable=get_details,
        op_kwargs={"location": "uk"},
    )

    uk_endpoints = XComArg(get_uk_endpoints)

    eu_extract = PythonOperator(
        task_id="extract-eu-data",
        python_callable=process_endpoints,
        op_kwargs={"locations": eu_endpoints, "env_vars": variables},
    )

    uk_extract = PythonOperator(
        task_id="extract-uk-data",
        python_callable=process_endpoints,
        op_kwargs={"locations": uk_endpoints, "env_vars": variables},
    )

    transform_mi = PythonOperator(
        task_id="tranform-eu-movement-indicators",
        python_callable=mi_transform,
        op_kwargs={"env_vars": variables},
    )

    transform_nd = PythonOperator(
        task_id="tranform-eu_cases-and-deaths",
        python_callable=nd_transform,
        op_kwargs={"env_vars": variables},
    )

    transform_vt = PythonOperator(
        task_id="tranform-eu_vaccine-tracker",
        python_callable=vt_transform,
        op_kwargs={"env_vars": variables},
    )

    transform_uk = PythonOperator(
        task_id="tranform-uk_placeholder",
        python_callable=uk_placeholder,
        op_kwargs={"env_vars": variables},
    )

    check_schema = PythonOperator(
        task_id="create-schema",
        python_callable=create_schema,
        op_kwargs={"env_vars": variables},
    )

    create_eu_data_model = PythonOperator(
        task_id="create-eu-data-model",
        python_callable=create_dimensional_model_eu,
        op_kwargs={"env_vars": variables},
    )

    create_uk_data_model = EmptyOperator(
        task_id="create-uk-data-model",
    )

    create_shared_data_model = EmptyOperator(
        task_id="create-shared-data-model",
    )

    load_shared_dims = PythonOperator(
        task_id="load-shared-dimensions",
        python_callable=maintain_shared_dims,
        op_kwargs={"env_vars": variables},
    )

    load_eu_dims = PythonOperator(
        task_id="load-eu-dimensions",
        python_callable=maintain_eu_dims,
        op_kwargs={"env_vars": variables},
    )

    load_uk_dims = PythonOperator(
        task_id="load-uk-dimensions",
        python_callable=maintain_uk_dims,
        op_kwargs={"env_vars": variables},
    )

    load_eu_facts = PythonOperator(
        task_id="load-eu-facts",
        python_callable=maintain_eu_facts,
        op_kwargs={"env_vars": variables},
    )

    load_uk_facts = PythonOperator(
        task_id="load-uk-facts",
        python_callable=maintain_uk_facts,
        op_kwargs={"env_vars": variables},
    )


start >> setup_environment
setup_environment >> get_eu_endpoints >> eu_extract
setup_environment >> get_uk_endpoints >> uk_extract
setup_environment >> check_schema
check_schema >> create_eu_data_model
check_schema >> create_uk_data_model
check_schema >> create_shared_data_model
eu_extract >> transform_mi >> eu_transform_complete
eu_extract >> transform_nd >> eu_transform_complete
eu_extract >> transform_vt >> eu_transform_complete
uk_extract >> transform_uk >> uk_transform_complete

eu_transform_complete >> load_eu_dims
create_eu_data_model >> load_eu_dims

uk_transform_complete >> load_uk_dims
create_uk_data_model >> load_uk_dims

eu_transform_complete >> load_shared_dims
uk_transform_complete >> load_shared_dims
create_shared_data_model >> load_shared_dims

load_eu_dims >> eu_dims_complete
load_shared_dims >> eu_dims_complete
load_shared_dims >> uk_dims_complete
load_uk_dims >> uk_dims_complete
eu_dims_complete >> load_eu_facts >> facts_complete
uk_dims_complete >> load_uk_facts >> facts_complete
facts_complete >> end
