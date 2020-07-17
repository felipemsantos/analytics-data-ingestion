import os
import random
import re
from datetime import datetime, timedelta
from uuid import uuid4

import boto3
from aws_lambda_powertools import Tracer, Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from faker import Faker

tracer = Tracer()
logger = Logger()
metrics = Metrics()

# Boto3 session configuration
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
AWS_REGION = os.getenv("AWS_REGION", None)
boto3.setup_default_session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

DDB_TABLE = os.getenv("DDB_TABLE", None)
ddb_table = boto3.resource("dynamodb").Table(DDB_TABLE)
fake = Faker("pt_BR")

@metrics.log_metrics
@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        record_count = 0
        logger.info("Generating dataset")
        for i in range(1000):
            random.seed(i)
            personType = random.randint(1, 2)
            document = re.sub("\.|\/|\-", "", fake.cpf() if personType == 1 else fake.cnpj())
            now = datetime.utcnow()
            record = {}
            record["document"] = document
            record["personType"] = personType
            record["name"] = fake.name()
            record["email"] = fake.email()
            record["phone"] = fake.msisdn()
            record["address"] = fake.address()
            record["addressNumber"] = fake.building_number()
            record["state"] = fake.estado_sigla()
            record["city"] = fake.city()
            record["neighborhood"] = fake.neighborhood()
            record["postalCode"] = fake.postcode()
            record["createdAt"] = str(now)
            record["updatedAt"] = str(now)
            record["subscriptions"] = []
            for j in range(3):
                random.seed(j)
                subscription = {}
                subscription["id"] = str(uuid4())
                subscription["charge"] = random.randint(10, 100)
                subscription["recurrenceMonths"] = random.randint(1, 12)
                subscription["paymentMethod"] = random.choice(["credit_card", "slip"])
                subscription["endAt"] = str(now + timedelta(days=(random.randint(1, 12) * 30)))
                subscription["startAt"] = str(now)
                subscription["status"] = random.choice(["active", "canceled"])
                subscription["createdAt"] = str(now)
                record["subscriptions"].append(subscription)
            ddb_table.put_item(Item=record)
            record_count += 1
        logger.info(f"{record_count} record(s) generated")
        metrics.add_metric(name="record_count", unit=MetricUnit.Count, value=record_count)
        return record_count
    except Exception as e:
        logger.error(e)
        raise e
