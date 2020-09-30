import os
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd
from aws_lambda_powertools import Tracer, Logger, Metrics
from boto3.dynamodb.types import TypeDeserializer

tracer = Tracer()
logger = Logger()
metrics = Metrics()

# Boto3 session configuration
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", None)
boto3.setup_default_session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
boto3_session = boto3._get_default_session()

TARGET_BUCKET = os.getenv("TARGET_BUCKET", None)
DATA_SOURCE = os.getenv("DATA_SOURCE", None)
TABLE_NAME = os.getenv("TABLE_NAME", None)


@metrics.log_metrics
@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        result = {}

        current_timestamp = datetime.utcnow()
        ingestion_date = current_timestamp.strftime("%Y-%m-%d")
        ingestion_time = current_timestamp.strftime("%H:%M:%S")

        type_deserializer = TypeDeserializer()

        customers_records = []
        subscription_records = []
        offers_records = []
        restrictions_records = []

        logger.info(f"Processing {len(event['Records'])} record(s)")
        for event_record in event["Records"]:
            raw = event_record["dynamodb"]["OldImage"] if event_record["eventName"] == "REMOVE" else event_record["dynamodb"]["NewImage"]
            op = event_record["eventName"][:1]
            record = {k: type_deserializer.deserialize(v) for k, v in raw.items()}
            record["ddb_op"] = op
            record["ddb_stream_date"] = ingestion_date
            record["ddb_stream_time"] = ingestion_time
            # define customer record
            customer = record

            # define subscriptions records
            for s in record['subscriptions']:
                subscription = s
                subscription['customer_id'] = customer['id']
                subscription["ddb_op"] = op
                subscription["ddb_stream_date"] = ingestion_date
                subscription["ddb_stream_time"] = ingestion_time

                for o in s['offers']:
                    offer = o
                    offer['subscription_id'] = subscription['id']
                    offer["ddb_op"] = op
                    offer["ddb_stream_date"] = ingestion_date
                    offer["ddb_stream_time"] = ingestion_time

                    for r in o['restrictions']:
                        restriction = r
                        restriction['offer_id'] = offer['id']
                        restriction["ddb_op"] = op
                        restriction["ddb_stream_date"] = ingestion_date
                        restriction["ddb_stream_time"] = ingestion_time
                        restrictions_records.append(restriction)

                    offer.pop('restrictions')
                    offers_records.append(offer)

                subscription.pop('offers')
                subscription_records.append(subscription)

            customer.pop('subscriptions')
            customers_records.append(customer)

        # Transforms the data list to pandas DataFrame
        customers_records_df = pd.DataFrame.from_dict(customers_records)
        subscription_records_df = pd.DataFrame.from_dict(subscription_records)
        offers_records_df = pd.DataFrame.from_dict(offers_records)
        restrictions_records_df = pd.DataFrame.from_dict(restrictions_records)

        # sanitize names
        data_source_name = wr.catalog._utils._sanitize_name(DATA_SOURCE)
        database_name = f"raw_{data_source_name}"

        # Creates catalog database doesn't exist
        try:
            logger.info(f"Creating catalog database {database_name}")
            wr.catalog.create_database(database_name)
        except Exception as e:
            logger.warning(e)

        customers_table_name = "customers_parquet"
        wr.s3.to_parquet(
            df=customers_records_df,
            path=f"s3://{TARGET_BUCKET}/raw/{data_source_name}/{customers_table_name}/",
            dataset=True,
            mode="append",
            partition_cols=["ddb_stream_date", "ddb_stream_time"],
            database=database_name,
            table=customers_table_name,
            catalog_versioning=True
        )
        logger.info(f"Fixing {database_name}.{customers_table_name} partitions")
        wr.athena.repair_table(customers_table_name, database_name)

        subscription_table_name = "subscriptions_parquet"
        wr.s3.to_parquet(
            df=subscription_records_df,
            path=f"s3://{TARGET_BUCKET}/raw/{data_source_name}/{subscription_table_name}/",
            dataset=True,
            mode="append",
            partition_cols=["ddb_stream_date", "ddb_stream_time"],
            database=database_name,
            table=subscription_table_name,
            catalog_versioning=True
        )
        logger.info(f"Fixing {database_name}.{subscription_table_name} partitions")
        wr.athena.repair_table(subscription_table_name, database_name)

        offers_table_name = "offers_parquet"
        wr.s3.to_parquet(
            df=offers_records_df,
            path=f"s3://{TARGET_BUCKET}/raw/{data_source_name}/{offers_table_name}/",
            dataset=True,
            mode="append",
            partition_cols=["ddb_stream_date", "ddb_stream_time"],
            database=database_name,
            table=offers_table_name,
            catalog_versioning=True
        )
        logger.info(f"Fixing {database_name}.{offers_table_name} partitions")
        wr.athena.repair_table(offers_table_name, database_name)

        restrictions_table_name = "restrictions_parquet"
        wr.s3.to_parquet(
            df=restrictions_records_df,
            path=f"s3://{TARGET_BUCKET}/raw/{data_source_name}/{restrictions_table_name}/",
            dataset=True,
            mode="append",
            partition_cols=["ddb_stream_date", "ddb_stream_time"],
            database=database_name,
            table=restrictions_table_name,
            catalog_versioning=True
        )
        logger.info(f"Fixing {database_name}.{restrictions_table_name} partitions")
        wr.athena.repair_table(restrictions_table_name, database_name)

        wr.athena.start_query_execution()

        wr.athena.get_query_execution()

        return result
    except Exception as e:
        logger.exception(e)
        raise e
