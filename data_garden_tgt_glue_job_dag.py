import time
import boto3
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
import ast

DAG_NAME = 'data_garden_tgt_glue_job_dag'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": dt.datetime(2021, 6, 16)
}


def get_tgt_task(config, **kwargs):
    config = ast.literal_eval(config)
    config = config['tgt_task'].lower().split(',')
    return config


def get_config_path(config, **kwargs):
    config = ast.literal_eval(config)
    config = config['config_path']
    return config


def run_glue_job(task, path, **kwargs):

    # Glue client.
    glue = boto3.client('glue', region_name='us-east-1')

    job_run = glue.start_job_run(JobName=task, Arguments={'--config-file-path': path})

    # Run until the job is succeeded, stopped  or failed.
    while True:
        # Delay to get the glue job status.
        time.sleep(60)
        # Get the current status of the glue job.
        current_status = glue.get_job_run(JobName=task, RunId=job_run['JobRunId'])['JobRun']['JobRunState']
        if current_status == 'SUCCEEDED':
            return f'Glue job:{task} {current_status}. Job ID: {job_run["JobRunId"]}'
        elif current_status == 'FAILED' or current_status == 'STOPPED':
            exit(1)


with DAG(
        DAG_NAME,
        default_args=default_args,
        catchup=False,
        schedule_interval=None
) as dag:

    get_config_path = PythonOperator(
        task_id='get_config_path',
        python_callable=get_config_path,
        provide_context=True,
        op_kwargs={'config': '{{ dag_run.conf}}'}
    )

    get_tgt_task = BranchPythonOperator(
        task_id='get_tgt_task',
        python_callable=get_tgt_task,
        provide_context=True,
        op_kwargs={'config': '{{ dag_run.conf}}'}
    )

    dg_targets_core = PythonOperator(
        task_id="core",
        python_callable=run_glue_job,
        provide_context=True,
        op_kwargs={'task': 'dg_targets_core',
                   'path': "{{ task_instance.xcom_pull(task_ids='get_config_path', key='return_value') }}"}
    )

    dg_targets_kids = PythonOperator(
        task_id="kids",
        python_callable=run_glue_job,
        provide_context=True,
        op_kwargs={'task': 'dg_targets_kids',
                   'path': "{{ task_instance.xcom_pull(task_ids='get_config_path', key='return_value') }}"}
    )

    dg_targets_plus = PythonOperator(
        task_id="plus",
        python_callable=run_glue_job,
        provide_context=True,
        op_kwargs={'task': 'dg_targets_plus',
                   'path': "{{ task_instance.xcom_pull(task_ids='get_config_path', key='return_value') }}"}
    )

    dg_targets_home = PythonOperator(
        task_id="home",
        python_callable=run_glue_job,
        provide_context=True,
        op_kwargs={'task': 'dg_targets_home',
                   'path': "{{ task_instance.xcom_pull(task_ids='get_config_path', key='return_value') }}"}
    )


get_config_path >> get_tgt_task >> [dg_targets_core, dg_targets_kids, dg_targets_plus, dg_targets_home]
