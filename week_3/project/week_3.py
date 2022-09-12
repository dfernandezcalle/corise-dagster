from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock

import copy


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    return ([Stock.from_list(i)
         for i in context.resources.s3.get_data(
            context.op_config["s3_key"])])


@op(
    ins={'stockList': In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)},
    description="Process S3 data: determine the Stock with the greatest high value from a List of Stock",
    tags={"kind": "s3"}
)
def process_data(stockList: List[Stock]) -> Aggregation:
    aggMax = max(stockList, key=lambda s: s.high)
    return Aggregation(
        date=aggMax.date,
        high=aggMax.high
    )


@op(
    required_resource_keys={"redis"},
    ins={'agg': In(dagster_type=Aggregation)},
    description="Persist data into Redis: receives an aggregation of data and persists it to Redis",
    tags={"kind": "redis"}
)
def put_redis_data(context, agg: Aggregation):
    context.resources.redis.put_data(
        name=agg.date.strftime("%m/%d/%Y"),
        value=agg.high
    )


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

def docker_setup(file: str):
    docker = {
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://host.docker.internal:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f"{file}"}}},
    }
    return docker

docker = docker_setup("prefix/stock_9.csv")

@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(key: str):
    return docker_setup(f"prefix/stock_{key}.csv")

local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor(job=docker_week_3_pipeline)
def docker_week_3_sensor(context):
    bucket = docker["resources"]["s3"]["config"]["bucket"]
    endpoint = docker["resources"]["s3"]["config"]["endpoint_url"]
    new_files = get_s3_keys(
        bucket=bucket,
        prefix='',
        endpoint_url=endpoint
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config=docker_setup(new_file))