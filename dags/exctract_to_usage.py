from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_python_script(script_name):
    """Function to run a Python script using the PythonOperator."""
    import subprocess
    subprocess.run(['python', script_name], check=True)

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'belib_data_processing_pipeline',
    default_args=default_args,
    description='A DAG to run data processing scripts for Belib',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Define tasks for each Python script
    extract_belib_donnees_stats = PythonOperator(
        task_id='extract_belib_donnees_stats',
        python_callable=run_python_script,
        op_kwargs={'script_name': 'extract_belib_donnees_stats.py'}
    )

    format_belib_donnees_stats = PythonOperator(
        task_id='format_belib_donnees_stats',
        python_callable=run_python_script,
        op_kwargs={'script_name': 'format_belib_donnees_stats.py'}
    )

    extract_belib_temps_reel = PythonOperator(
        task_id='extract_belib_temps_reel',
        python_callable=run_python_script,
        op_kwargs={'script_name': 'extract_belib_temps_reel.py'}
    )

    format_belib_temps_reel = PythonOperator(
        task_id='format_belib_temps_reel',
        python_callable=run_python_script,
        op_kwargs={'script_name': 'format_belib_temps_reel.py'}
    )

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=run_python_script,
        op_kwargs={'script_name': 'combine_data.py'}
    )

    # Setting up dependencies to reflect the provided DAG diagram
    extract_belib_donnees_stats >> format_belib_donnees_stats >> combine_data
    extract_belib_temps_reel >> format_belib_temps_reel >> combine_data

    combine_data >> index_to_elastic  # Assuming `index_to_elastic` is your Elastic indexing task

    # The "index_to_elastic" task needs to be defined if it is part of the workflow
    index_to_elastic = PythonOperator(
        task_id='index_to_elastic',
        python_callable=run_python_script,
        op_kwargs={'script_name': 'index_to_elastic.py'}  # Replace 'index_to_elastic.py' with the actual script name if different
    )