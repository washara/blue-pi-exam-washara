from sqlalchemy import engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,PythonVirtualenvOperator
from airflow.models import Variable
from datetime import datetime, timedelta

#for local used only and don't forget to add json keys to service keys path
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcloud_param["credential"]

from pipeline.services.extract_raw import extract_and_load
from pipeline.services.transform_and_load import import_to_bq
from pipeline.variable.parameter import pg_sql_param,gcloud_param,DB_URI


default_args = {
    'owner': 'washara',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'exam-bluepi-washara',
    default_args=default_args,
    description='Dag for bluepi-exam',
    schedule_interval="0 * * * *",
    start_date=datetime(2021, 6, 30),
    catchup=False,
    tags=['exam']
) as dag:
    for table in pg_sql_param["table_list"]:
        t1 = PythonVirtualenvOperator(task_id='extract_and_upload_' + table,python_callable=extract_and_load,op_args=[table,DB_URI,pg_sql_param,gcloud_param],provide_context=True,requirements=["SQLAlchemy==1.4.20","pandas==1.2.5","google-cloud-storage==1.39.0","psycopg2-binary==2.9.1"],system_site_packages=False)
        t2 = PythonVirtualenvOperator(task_id='transform_to_bq_'+ table,python_callable=import_to_bq,op_args=[table,pg_sql_param,gcloud_param],provide_context=True,requirements=["SQLAlchemy==1.4.20","pandas==1.2.5","google-cloud-storage==1.39.0","google-cloud-bigquery==2.20.0","fsspec==2021.6.1","gcsfs==2021.6.1"],system_site_packages=False)
        t1 >> t2