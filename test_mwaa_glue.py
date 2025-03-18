import boto3
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import time

DAG_NAME = 'TEST_MWAA_GLUE'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": dt.datetime(2024, 1, 24)
}


def check_glue_task_status(task):

    glue_client = boto3.client('glue', region_name='us-east-1')

    job_run_id = None
    response = glue_client.get_job_runs(JobName=task)
    for res in response['JobRuns']:
        if res.get("JobRunState") == "RUNNING":
            print("Job Run id is:" + res.get("Id"))
            print("status is:" + res.get("JobRunState"))
            job_run_id = res.get("Id")
            break
    return job_run_id



def execute_glue_job(config, **kwargs):
    print(config)
    glue = boto3.client('glue', region_name='us-east-1')
    job_run = glue.start_job_run(JobName="dg_source_audits")
    print("###############################")
    print(job_run)
    time.sleep(30)
    job_run_id = check_glue_task_status("dg_source_audits")
    print("###############################")
    print(job_run_id)
    print("###############################")
    time.sleep(30)
    # Run until the job is succeeded, stopped  or failed.
    while True:
        # Delay to get the glue job status.
        time.sleep(20)
        # Get the current status of the glue job.
        current_status = glue.get_job_run(JobName="dg_source_audits", RunId=job_run_id)['JobRun']['JobRunState']
        print(current_status)
        print("###############################")
    

with DAG(
        DAG_NAME,
        default_args=default_args,
        catchup=False,
        schedule_interval=None
) as dag:

    start_task = DummyOperator(task_id='start_task')

    get_config_dg_glue_task = PythonOperator(
        task_id="get_config_dg_glue",
        python_callable=execute_glue_job,
        provide_context=True,
        op_kwargs={'config': '{{ dag_run.conf }}'}
    )

    end_task = DummyOperator(task_id='end_task')

    # Define the task dependencies
    start_task >> get_config_dg_glue_task >> end_task

