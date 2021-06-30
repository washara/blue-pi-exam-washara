from airflow.models import Variable

pg_sql_param = Variable.get("pg_sql_param", deserialize_json=True)
gcloud_param = Variable.get("gcloud_param", deserialize_json=True)

pg_uri_param = Variable.get("pg_uri_authorization", deserialize_json=True)

DB_URI = "postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}".format(
    pg_user=pg_uri_param["username"],
    pg_pass=pg_uri_param["password"],
    pg_host=pg_uri_param["hostname"],
    pg_port=pg_uri_param["port"],
    pg_db=pg_uri_param["dbname"],
)