import os
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
from aws_lambda_powertools import Tracer, Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from dynamodb_json import json_util as ddb_json

tracer = Tracer()
logger = Logger()
metrics = Metrics()

# Boto3 session configuration
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", None)
boto3.setup_default_session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

TARGET_BUCKET = os.getenv("TARGET_BUCKET", None)
DATA_SOURCE = os.getenv("DATA_SOURCE", None)
TABLE_NAME = os.getenv("TABLE_NAME", None)

@metrics.log_metrics
@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        records = []
        customers = []
        subscriptions = []
        result = {}

        current_timestamp = datetime.utcnow()
        ingestion_date = current_timestamp.strftime("%Y-%m-%d")
        ingestion_time = current_timestamp.strftime("%H:%M:%S")

        logger.info(f"Processing {len(event['Records'])} record(s)")
        for event_record in event["Records"]:
            raw = event_record["dynamodb"]["OldImage"] if event_record["eventName"] == "REMOVE" else \
                event_record["dynamodb"]["NewImage"]
            op = event_record["eventName"][:1]
            record = ddb_json.loads(raw)
            record["op"] = op
            record["ingestion_date"] = ingestion_date
            record["ingestion_time"] = ingestion_time
            records.append(record)

        # Transforms the data list to pandas DataFrame
        records_df = pd.DataFrame.from_dict(records)

        result["records_count"], _ = records_df.shape
        metrics.add_metric(name="records_count",
                           unit=MetricUnit.Count,
                           value=result["records_count"])

        # sanitize names
        data_source_name = wr.catalog._utils._sanitize_name(DATA_SOURCE)
        table_name = f"{wr.catalog._utils._sanitize_name(TABLE_NAME)}_parquet"
        database_name = f"raw_{data_source_name}"

        # Creates catalog database doesn't exist
        try:
            logger.info(f"Creating catalog database {database_name}")
            wr.catalog.create_database(database_name)
        except Exception as e:
            logger.warning(e)

        table_dataset_path = f"s3://{TARGET_BUCKET}/raw/{data_source_name}/{table_name}/"
        logger.info(f"Persisting customers DataFrame at table {database_name}.{table_name}")
        wr.s3.to_parquet(
            df=records_df,
            path=table_dataset_path,
            dataset=True,
            mode="append",
            partition_cols=["ingestion_date", "ingestion_time"],
            database=database_name,
            table=table_name,
            catalog_versioning=True
        )
        logger.info(f"Fixing {database_name}.{table_name} partitions")
        wr.athena.repair_table(table_name, database_name)

        return result
    except Exception as e:
        logger.error(e)
        raise e
