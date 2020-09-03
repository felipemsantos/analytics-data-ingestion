import json

import pytest

from stream_processor import app
from .mock import MockContext


@pytest.fixture()
def ddb_table_stream_event():
    """ Generates DynamoDB TableStrem Event"""
    with open("../../events/stream_processor_event.json", "r") as fp:
        return json.load(fp)


def test_lambda_handler(ddb_table_stream_event):
    context = MockContext(__name__)
    ret = app.lambda_handler(ddb_table_stream_event, context)
    assert ret["record_count"] == len(ddb_table_stream_event["Records"])
    assert ret["partitions"] > 0
