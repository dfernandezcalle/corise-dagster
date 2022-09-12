from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    group_name="corise",
)
def get_s3_data(context):
    return ([Stock.from_list(i)
        for i in context.resources.s3.get_data(
            context.op_config["s3_key"])])


@asset(
    group_name="corise",
)
def process_data(stockList: List[Stock]) -> Aggregation:
    aggMax = max(stockList, key=lambda s: s.high)
    return Aggregation(
        date=aggMax.date,
        high=aggMax.high
    )


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
)
def put_redis_data(context, agg: Aggregation):
    context.resources.redis.put_data(
        name=agg.date.strftime("%m/%d/%Y"),
        value=agg.high
    )


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    }
)