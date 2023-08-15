from google.cloud import bigquery, pubsub_v1, storage


def export_bigquery_table_to_gcs(logger, bucket_name, blob_folder, dataset, table_id):
    """
    Export bigquery to gcs bucket!
    :param logger:
    :param bucket_name:
    :param blob_folder:
    :param dataset:
    :param table_id:
    :return:
    """
    bq_client = bigquery.Client()
    storage_client = storage.Client()

    destination_uri = "gs://{}/{}".format(bucket_name,
                                          f"{blob_folder}/raw_data_*.parquet")
    blob_name = f"{blob_folder}/raw_data_000000000000.parquet"

    if is_gcs_object_exists(bucket_name, blob_name) == True:
        print(f"Target Blob Already Exists: {bucket_name}/{blob_name}")
        return "target_blob_exists"

    table_ref = f"{dataset}.{table_id}"
    print(table_ref)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.ParquetOptions
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.PARQUET

    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config=job_config,
    )  # API request

    if not extract_job.result():
        logger.error(f"Job export bigquery table to gcs failed. Job id: {extract_job.job_id}")
        raise Exception(f"Job export bigquery table to gcs failed. Job id: {extract_job.job_id}")

    return None


def is_gcs_object_exists(bucket_name, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    status = storage.Blob(
        bucket=bucket,
        name=file_path
    ).exists(client)
    return status


def write_workflow_target(file_path, content, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob_obj = storage.Blob(file_path, bucket)
    blob_obj.upload_from_string(content)
