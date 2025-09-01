"""Transform Covid data for UK data"""

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from load import (
    create_schema,
    maintain_shared_dims,
    maintain_uk_dims,
    maintain_uk_facts,
)
from utils import get_set_config

with DAG(
    dag_id="load-uk-data",
    description="Load UK transformed data and output to database",
    render_template_as_native_obj=True,
) as load_uk_dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    model_created = EmptyOperator(task_id="data-model-created")
    uk_dims_complete = EmptyOperator(task_id="uk-dimensions-end")
    facts_complete = EmptyOperator(task_id="fact-tables-end")

    setup_environment = PythonOperator(
        task_id="get-environment-details",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    variables = XComArg(setup_environment)

    check_schema = PythonOperator(
        task_id="create-schema",
        python_callable=create_schema,
        op_kwargs={"env_vars": variables},
    )

    create_uk_data_model = EmptyOperator(
        task_id="create-uk-data-model",
    )

    load_shared_dims = PythonOperator(
        task_id="load-shared-dimensions",
        python_callable=maintain_shared_dims,
        op_kwargs={"env_vars": variables},
    )

    load_uk_dims = PythonOperator(
        task_id="load-uk-dimensions",
        python_callable=maintain_uk_dims,
        op_kwargs={"env_vars": variables},
    )

    load_uk_facts = PythonOperator(
        task_id="load-uk-facts",
        python_callable=maintain_uk_facts,
        op_kwargs={"env_vars": variables},
    )


start >> setup_environment >> check_schema >> create_uk_data_model >> model_created
model_created >> load_uk_dims >> uk_dims_complete
model_created >> load_shared_dims >> uk_dims_complete
uk_dims_complete >> load_uk_facts >> facts_complete >> end
