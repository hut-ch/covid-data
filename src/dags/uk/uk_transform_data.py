"""Transform Covid data for UK data"""

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from transform import uk_placeholder
from utils import get_set_config

with DAG(
    dag_id="transform-uk-data",
    description="Transforms UK extracted data and outputs data in json format",
    render_template_as_native_obj=True,
) as transform_all_dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    setup_environment = PythonOperator(
        task_id="get-environment-details",
        python_callable=get_set_config,
        op_args=[".env"],
    )

    variables = XComArg(setup_environment)

    transform_uk = PythonOperator(
        task_id="tranform-uk_cplaceholder",
        python_callable=uk_placeholder,
        op_kwargs={"env_vars": variables},
    )

start >> setup_environment >> transform_uk >> end
