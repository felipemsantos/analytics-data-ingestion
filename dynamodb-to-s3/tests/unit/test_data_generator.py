import json
import pytest
from .mock import MockContext
from data_generator import app


@pytest.fixture()
def cloudwatch_scheduler_event():
    """ Generates CloudWatch Scheduler Event"""
    with open("./events/data_generator_event.json", "r") as fp:
        return json.load(fp)


def test_lambda_handler(cloudwatch_scheduler_event):
    ret = app.lambda_handler(cloudwatch_scheduler_event, MockContext(__name__))
    assert ret > 0
