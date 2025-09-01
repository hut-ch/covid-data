"""Transform Covid data for All data"""

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from transform import mi_transform, nd_transform, uk_placeholder, vt_transform
from utils import get_set_config

with DAG(
    dag_id="transform-all-data",
    description="Transforms All extracted data and outputs data in json format",
    render_template_as_native_obj=True,
) as transform_all_dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    eu_complete = EmptyOperator(task_id="eu-transform-end")

    setup_environment = PythonOperator(
        task_id="get-environment-details",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    variables = XComArg(setup_environment)

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


start >> setup_environment >> transform_mi >> eu_complete
start >> setup_environment >> transform_nd >> eu_complete
start >> setup_environment >> transform_vt >> eu_complete
eu_complete >> transform_uk >> end
