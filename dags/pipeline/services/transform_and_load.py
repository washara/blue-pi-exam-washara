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
            df[col_name] = df[col_name].astype("str")
            df[new_col["new_col_name"]] = df[col_name].map(new_col["mapping_value"])
            new_df = df.drop(columns=[col_name])
    else:
        new_df = df

    client = bigquery.Client()
    table_ref = client.dataset(gcloud_param["bigquery"]["dataset"]).table(gcloud_param["bigquery"]["table_mapping"][table])
    table = client.get_table(table_ref)
    client.insert_rows_from_dataframe(table, new_df)
    