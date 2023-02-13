from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta

from tools.api_tools import import_data_to_json
from tools.s3_tools import clear_s3_folder
from scripts.import_nhl import load_data_nhl

ENDPOINT_URL = "https://statsapi.web.nhl.com/api/v1/teams/21/stats"

dag = DAG(
    "import_nhl",
    start_date=datetime(2023, 2, 13, 0, 0, 0),
    description="Импорт данных из NHL",
    default_args={
        "owner": "test",
        "task_concurrency": 1,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    schedule_interval="0 */12 * * *",
    tags=["import", "nhl", "api", "every 12 hours"],
    catchup=False,
    orientation="LR"
)

start_op = DummyOperator(
    task_id="start_import_pipeline",
    dag=dag
)

import_data_to_json_op = PythonOperator(
    task_id="import_nhl_data",
    python_callable=import_data_to_json,
    provide_context=True,
    op_kwargs={
        'url': ENDPOINT_URL,
        'path': '../s3/nhl',
        'file_name': f'nhl_{datetime.now()}'
    },
    dag=dag
)

load_data_nhl_op = PythonOperator(
    task_id="load_data_nhl",
    python_callable=load_data_nhl,
    provide_context=True,
    dag=dag
)

clear_s3_folder_op = PythonOperator(
    task_id="clear_s3_folder",
    python_callable=clear_s3_folder,
    provide_context=True,
    dag=dag
)

end_op = DummyOperator(
    task_id="finish_import_pipeline",
    dag=dag
)

start_op >> import_data_to_json_op >> load_data_nhl_op >> clear_s3_folder_op >> end_op
