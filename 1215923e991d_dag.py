from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    '1215923e991d_dag',  
    default_args=default_args,
    description='Trigger Databricks Notebook',
    schedule_interval='@daily',  
    start_date=datetime(2025, 2, 26), 
    catchup=False,
)


notebook_run_task = DatabricksRunNowOperator(
    task_id='trigger_databricks_notebook',
    databricks_conn_id='databricks_default',  
    job_id=985068857536828,  
    dag=dag,
)


notebook_run_task
