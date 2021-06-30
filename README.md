# blue-pi-exam-washara

# For Local Testing 
# Setup Docker
## Docker Build
docker build -t airflow-exam:1.0 .

## Docker Run
for local testing only create service keys in cloud iam and mount dags folder and service keys folder on docker run \

docker run -itd --name airflow-exam -p 8080:8080 -v {path_to_local_dags}:/root/airflow/dags -v {path_to_servicekeys}:/root/airflow/resources/service-keys airflow-exam:1.0

access through http://localhost:8080

# For Production
use cloud composer on gcp

upload to cloud storage dags folder `us-central1-bluepi-airflow--b1d03777-bucket/dags`

access through https://id2e29a7cd9fafb80p-tp.appspot.com/home

# Variables
variables are set-up on airflow console variables tab this pipeline have 3 group of variables:

1.  pg_uri_authorization used for connect to postgres database
2.  pg_sql_param used for extract data from postgres and transform value mapping
3.  gcloud_param used for interact with resource and mapping table on gcp

pg_uri_authorization = { \
    "username": "exam", \
    "password": "bluePiExam", \
    "hostname": "35.247.174.171", \
    "port": "5432", \
    "dbname": "postgres"}

pg_sql_param = {
    "schema": "public", \
    "table_list": ["users", "user_log"], \
    "table_to_transform": {"user_log": {"status": {"new_col_name":"success","mapping_value":{"0": "False", "1": "True"}}}}
}

gcloud_param = { \
    "credentials": "path/to/servicekeys", \
    "storage": { 
        "bucket": "exam-bluepi-bucket-washara", 
        "storage_path": "raw_data" 
    }, \
    "bigquery": { 
        "dataset": "user_dataset", 
        "table_mapping":{ 
            "users":"users", 
            "user_log":"user_log" 
            }
            }
}

# How to work with jupyter notebook on Dataproc
follow below link and go to GCS folder
https://niumdztfee5tvgajfegofzg6z4-dot-us-central1.dataproc.googleusercontent.com/gateway/default/jupyter/tree?
