from airflow import DAG
from airflow.operators.python_operator import PythonOperator,PythonVirtualenvOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os

#get variables
pg_uri_param = Variable.get("pg_uri_authorization", deserialize_json=True)
pg_sql_param = Variable.get("pg_sql_param", deserialize_json=True)
gcloud_param = Variable.get("gcloud_param", deserialize_json=True)
service_credentials = Variable.get("service_credentials", deserialize_json=True)
POSTGRES_DB_URI = "postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}".format(
    pg_user=pg_uri_param["username"],
    pg_pass=pg_uri_param["password"],
    pg_host=pg_uri_param["hostname"],
    pg_port=pg_uri_param["port"],
    pg_db=pg_uri_param["dbname"],
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_credentials["path_to_keys"]

def export_raw_data(table,POSTGRES_DB_URI,pg_sql_param,gcloud_param,**context):
    from sqlalchemy import create_engine
    from pandas import read_sql
    from google.cloud import storage
    
    engine = create_engine(POSTGRES_DB_URI)
    sql_query = "SELECT * FROM {schema_name}.{table_name}".format(schema_name=pg_sql_param["schema"], table_name=table)
    df = read_sql(sql_query, engine)
    filename = table + "_" + context['ts_nodash']
    client = storage.Client()
    bucket = client.get_bucket(gcloud_param["storage"]["bucket"])
    bucket.blob(
        "{storage_path}/{filename}.csv".format(
            storage_path=gcloud_param["storage"]["storage_path"],
            filename=filename
        )
    ).upload_from_string(df.to_csv(index=False), "text/csv")

def import_to_bq(table,pg_sql_param,gcloud_param,**context):
    from pandas import read_csv
    from google.cloud import bigquery

    filename = table + "_" + context['ts_nodash']
    file_path = "gs://{bucket_name}/{storage_path}/{filename}.csv".format(
        bucket_name=gcloud_param["storage"]["bucket"],
        storage_path=gcloud_param["storage"]["storage_path"],
        filename=filename
        )
    df = read_csv(file_path)
    if table in pg_sql_param["table_to_transform"]:
        for col_name,new_col in pg_sql_param["table_to_transform"][table].items():
            df[new_col["new_col_name"]] = df[col_name].map(new_col["mapping_value"])
            new_df = df.drop(columns=[col_name])
    else:
        new_df = df
    client = bigquery.Client()
    table_ref = client.dataset(gcloud_param["bigquery"]["dataset"]).table(gcloud_param["bigquery"]["table_mapping"][table])
    table = client.get_table(table_ref)
    client.insert_rows_from_dataframe(table, new_df)

default_args = {
    'owner': 'washara',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'exam-bluepi-washara',
    default_args=default_args,
    description='Dag for bluepi-exam',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2021, 6, 30),
    catchup=False,
    tags=['exam'],
) as dag:
    for table in pg_sql_param["table_list"]:
        t1 = PythonVirtualenvOperator(task_id='extract_and_upload_' + table,python_callable=export_raw_data,op_args=[table,POSTGRES_DB_URI,pg_sql_param,gcloud_param],provide_context=True,requirements=["SQLAlchemy==1.4.20","pandas==1.2.5","google-cloud-storage==1.39.0","psycopg2-binary==2.9.1"],system_site_packages=False)
        t2 = PythonVirtualenvOperator(task_id='transform_to_bq_'+ table,python_callable=import_to_bq,op_args=[table,pg_sql_param,gcloud_param],provide_context=True,requirements=["SQLAlchemy==1.4.20","pandas==1.2.5","google-cloud-storage==1.39.0","google-cloud-bigquery==2.20.0","fsspec==2021.6.1","gcsfs==2021.6.1"],system_site_packages=False)
        t1 >> t2