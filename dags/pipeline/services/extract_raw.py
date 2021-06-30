def extract_and_load(table,DB_URI,pg_sql_param,gcloud_param,**context):
    from sqlalchemy import create_engine
    from pandas import read_sql
    from google.cloud import storage

    engine = create_engine(DB_URI)
    
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

