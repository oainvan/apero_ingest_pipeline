from google.cloud import bigquery
import pandas as pd
from pytz import timezone
import yaml


def upload_df_to_bigquery(logger, df_upload, table_upload):
    """
    Upload data frame to bigquery table.

    :param logger:
    :param df_upload:
    :param table_upload:
    :return:
    """

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    job_config.labels = {
        "requestor": "marketing_ingest_pipeline",
        "category": "upload_df_to_bigquery",
    }
    job = client.load_table_from_dataframe(
        df_upload, table_upload, job_config=job_config
    )
    if not job.result():
        logger.error(f"Job upload df to bigquery failed. Job id: {job.job_id}")
        raise Exception(f"Job upload df to bigquery failed. Job id: {job.job_id}")
    logger.info(f'Job upload df to bigquery done: Table upload : {table_upload}')


def get_bq_table_to_dataframe(sql, labels):
    """
    Read bigquery table to dataframe

    :param labels:
    :param sql:
    :return:
    """
    bq_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    job_config.labels = labels
    return bq_client.query(sql, job_config=job_config).result().to_dataframe()


def is_partition_exists(logger, table_ref, partition_value):
    """
    Check partition if not exists in table.
    :param partition_value:
    :param table_ref:
    :param logger:
    :return:
    """

    client = bigquery.Client()
    partitions = client.list_partitions(table_ref)
    logger.info(f"List partitions of {table_ref}: {partitions}")
    if partition_value.replace("-", "") in partitions:
        logger.warning(f'\nPartitions: {partition_value} is exists in {table_ref}.')
        return True
    else:
        logger.info(f'\nPartitions: {partition_value} is not exists in {table_ref}.')
        return False


def get_bq_table_ref(project_id, dataset_id, table_name):
    """
    Get table ref to project and dataset

    :param project_id:
    :param dataset_id:
    :param table_name:
    :return:
    """

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_name)

    return table_ref


def bq_query(sql, labels, table_ref=None):
    """
    Send SQL to BigQuery

    :param labels:
    :param sql:
    :param table_ref: if set, write results to this table
    :return:
    """
    bq_client = bigquery.Client()

    job_config = bigquery.QueryJobConfig()
    if table_ref:
        job_config.allow_large_results = True
        job_config.destination = table_ref
        job_config.write_disposition = 'WRITE_APPEND'

    job_config.labels = labels
    return bq_client.query(sql, job_config=job_config)


def get_list_table_of_dataset(dataset_id):
    """
    Get list table raw data of dataset on bigquery
    :param dataset_id:
    :return:
    """

    bq_client = bigquery.Client()
    tables = bq_client.list_tables(dataset_id)
    list_tables = []
    for table in tables:
        list_tables.append(table.table_id)
    return list_tables


def bq_create_table(table_ref, table_path, partition_field=None, cluster=None):
    """
    Create Bigquery Table if not exists.
    :param table_path:
    :param cluster:
    :param table_ref:
    :param partition_field:
    :return:
    """

    bq_client = bigquery.Client()

    schema = build_bq_table_schema(table_path)

    table = bigquery.Table(table_ref, schema=schema)
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        if cluster:
            table.clustering_fields = cluster

    bq_client.create_table(table, exists_ok=True)


def build_bq_table_schema(table_path):
    """
    Convert yaml data to BigQuery Schema format

    :param table_path:
    :return:
    """

    with open(f"{table_path}.yaml", 'r') as f:
        schema = yaml.load(f, Loader=yaml.SafeLoader)

    return [bigquery.SchemaField(col[0], col[1]) for col in schema]


def build_raw_table_schema(table_path):
    """
    Convert yaml data to Bigquery Schema
    :param table_path:
    :return:
    """

    with open(f"{table_path}.yaml", "r") as file:
        schema_yaml = file.read()
    schema_dict = yaml.safe_load(schema_yaml)

    def process_fields(fields):
        processed_fields = []
        for field in fields:
            field_name = list(field.keys())[0]
            field_info = list(field.values())[0]
            if field_info["type"] == "RECORD":
                processed_fields.append(
                    bigquery.SchemaField(
                        field_name,
                        field_type=field_info["type"],
                        mode=field_info.get("mode", "NULLABLE"),
                        fields=process_fields(field_info["fields"]),
                    )
                )
            else:
                processed_fields.append(
                    bigquery.SchemaField(
                        field_name,
                        field_type=field_info["type"],
                        mode=field_info.get("mode", "NULLABLE"),
                    )
                )
        return processed_fields

    schema_fields = process_fields(schema_dict["fields"])
    return schema_fields


def is_table_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except:
        return False
