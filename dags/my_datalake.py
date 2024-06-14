from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.format_belib_real_time import formatting_real_time
from lib.format_belib_static_data import formatting_stats
from lib.extract_belib_static_data import extract_stats
from lib.extract_belib_real_time import extract_real_time
from lib.combine_data import formatting_combine
from lib.elastic import send_elastic

#DAG pour Airflow, avec nos 6 tâches
with DAG(
       'my_datalake',
       params={'fake_param_to_have_conf_in_ui' : 123},
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(seconds=15),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """

task_1 = PythonOperator(
    task_id='extract_stats',
    python_callable=extract_stats,
    op_kwargs={'task_number': 'task1'},
    provide_context=True,
    dag=dag
)

task_2 = PythonOperator(
    task_id='extract_real_time',
    python_callable=extract_real_time,
    op_kwargs={'task_number': 'task2'},
    provide_context=True,
    dag=dag
)

task_3 = PythonOperator(
    task_id='formatting_stats',
    python_callable=formatting_stats,
    op_kwargs={'task_number': 'task3'},
    provide_context=True,
    dag=dag
)

task_4 = PythonOperator(
    task_id='formatting_real_time',
    python_callable=formatting_real_time,
    op_kwargs={'task_number': 'task4'},
    provide_context=True,
    dag=dag
)

task_5 = PythonOperator(
    task_id='formatting_combine',
    python_callable=formatting_combine,
    op_kwargs={'task_number': 'task5'},
    provide_context=True,
    dag=dag
)

task_6 = PythonOperator(
    task_id='send_elastic',
    python_callable=send_elastic,
    op_kwargs={'task_number': 'task6'},
    provide_context=True,
    dag=dag
)

task_1 >> task_3 >> task_5
task_2 >> task_4 >> task_5
task_5 >> task_6
