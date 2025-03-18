import boto3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
import datetime
import time
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": datetime.datetime(2024, 1, 24)
}

def check_glue_task(task):
    glue_client=boto3.client('glue',region_name='us-east-1')
    response=glue_client.get_job_runs(JobName=task)
    for res in response['JobRuns']:
        if res.get('JobRunState')=='RUNNING':
            print('Job run id is: ',res.get('Id'))
            print("status is:",res.get('JobRunState'))
            job_run_id=res.get('Id')
            break
    return job_run_id

def execute_glue_job(config,**kwargs):
    print(config)
    glue=boto3.client('glue',region_name='us-east-1')
    job=glue.start_job_run(JobName='testygh')
    print("###############################")
    print(job)
    time.sleep(30)
    job_run_id=check_glue_task('testygh')
    print("###############################")
    print(job_run_id)
    print("###############################")
    time.sleep(30)
    while True:
        time.sleep(20)
        # Get the current status of the glue job.
        current_status=glue.get_job_run(JobName='testygh',RunId=job_run_id)['JobRun']['JobRunState']
        print(current_status)
        print("###############################")
with DAG(dag_id='Glue_test',default_args=default_args,schedule_interval=None,catchup=False) as dag:
    start_job=DummyOperator(task_id='start_job')
    execute_job=PythonOperator(task_id='execute_job',python_callable=execute_glue_job,
                               provide_context=True,op_kwargs={'config':'{{dag_run.conf}}'})
    end_job=DummyOperator(task_id='end_job')
    start_job>>execute_job>>end_job


