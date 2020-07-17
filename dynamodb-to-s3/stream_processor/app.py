import json
import os

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

BUCKET = os.getenv("BUCKET", None)
DATA_SOURCE = os.getenv("DATA_SOURCE", None)
TABLE_NAME = os.getenv("TABLE_NAME", None)
PARTITION_COLS = os.getenv("PARTITION_COLS", None)
CATALOG_DATABASE = os.getenv("CATALOG_DATABASE", None)
CATALOG_TABLE_NAME = os.getenv("CATALOG_TABLE_NAME", None)

@metrics.log_metrics
@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        records = []
        result = {}
        logger.info(f"Processing {len(event['Records'])} record(s)")
        for event_record in event["Records"]:
            raw = event_record["dynamodb"]["OldImage"] if event_record["eventName"] == "REMOVE" else event_record["dynamodb"]["NewImage"]
            record = ddb_json.loads(raw)
            record["op"] = event_record["eventName"][:1]
            records.append(record)

        df = pd.DataFrame.from_dict(records)
        result["record_count"], _ = df.shape
        metrics.add_metric(name="record_count", unit=MetricUnit.Count, value=result["record_count"])

        table_name = wr.catalog.sanitize_table_name(CATALOG_TABLE_NAME)
        logger.info(f"Storing dataframe at table {CATALOG_DATABASE}.{table_name}")
        s3_to_parquet = wr.s3.to_parquet(
            df=df,
            path=f"s3://{BUCKET}/raw/{DATA_SOURCE}/{TABLE_NAME}/",
            dataset=True,
            partition_cols=PARTITION_COLS.split(",") if PARTITION_COLS else None,
            database=CATALOG_DATABASE,
            table=table_name,
            catalog_versioning=True
        )
        result["partitions"] = len(s3_to_parquet["partitions_values"])
        metrics.add_metric(name="partitions", unit=MetricUnit.Count, value=result["partitions"])

        logger.info(f"Fixing table {CATALOG_DATABASE}.{table_name} partition(s) {PARTITION_COLS}")
        wr.athena.repair_table(table_name, CATALOG_DATABASE)
        return result
    except Exception as e:
        logger.error(e)
        raise e
