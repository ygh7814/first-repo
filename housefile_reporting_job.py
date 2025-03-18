import boto3
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import ast
import time

DAG_NAME = 'housefile_reporting_job'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": dt.datetime(2021, 11, 10)
}


def get_config_dg_glue(config, **kwargs):
    return config


def run_glue_job(task, config, status_check_time=60, **kwargs):

    print(f'CONFIG : {config}')

    config = ast.literal_eval(config)

    run_date = config['run_date'] if config is not None and 'run_date' in config else False

    custom_run_parameter = config['custom_run_parameter'] \
        if config is not None and 'custom_run_parameter' in config else False

    print(f'TASK : {task}')
    print(f'Run_Date : {run_date}')
    print(f'Custom_run_parameter : {custom_run_parameter}')

    # Glue client.
    glue = boto3.client('glue', region_name='us-east-1')

    if run_date:
        job_run = glue.start_job_run(JobName=task, Arguments={'--run_date': run_date,
                                                              '--custom_run_parameter': custom_run_parameter})
    else:
        job_run = glue.start_job_run(JobName=task)

    # Run until the job is succeeded, stopped  or failed.
    while True:
        # Delay to get the glue job status.
        time.sleep(status_check_time)
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
        schedule_interval='0 11 * * *'
) as dag:

    get_config_dg_glue = PythonOperator(
        task_id="get_config_dg_glue",
        python_callable=get_config_dg_glue,
        provide_context=True,
        op_kwargs={'config': '{{ dag_run.conf}}'}
    )

    reports_backend = PythonOperator(
        task_id="reports_backend",
        python_callable=run_glue_job,
        provide_context=True,
        op_kwargs={'task': 'housefile_reports_backend',
                   'config': "{{ task_instance.xcom_pull(task_ids='get_config_dg_glue', key='return_value') }}"}
    )

    reports_ui = PythonOperator(
        task_id="reports_ui",
        python_callable=run_glue_job,
        provide_context=True,
        op_kwargs={'task': 'housefile_reports_frontend',
                   'config': "{{ task_instance.xcom_pull(task_ids='get_config_dg_glue', key='return_value') }}"}
    )


get_config_dg_glue >> reports_backend >> reports_ui
