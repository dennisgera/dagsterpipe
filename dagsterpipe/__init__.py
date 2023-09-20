import os
from datetime import date, datetime
from decimal import Decimal
from json import JSONEncoder
from uuid import UUID

from dagster import create_repository_using_definitions_args, in_process_executor

from dagsterpipe.assets import (
    financial_market_assets
)
from dagsterpipe.jobs import all_jobs
from dagsterpipe.resources import RESOURCES_LOCAL, RESOURCES_PROD
from dagsterpipe.schedules import all_schedules
from dagsterpipe.sensors import all_sensors

default_json_encoder = JSONEncoder.default


def custom_json_encoder(self, obj):
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return default_json_encoder(self, obj)


JSONEncoder.default = custom_json_encoder


resources_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
}


def get_deployment_name() -> str:
    if os.getenv("DAGSTER_DEPLOYMENT", "") == "prod":
        return "prod"
    return "local"


all_assets = [    
    *financial_market_assets,
]

defs = create_repository_using_definitions_args(
    name="dagarado",
    assets=all_assets,
    resources=resources_by_deployment_name[get_deployment_name()],
    jobs=all_jobs,
    executor=in_process_executor,
    schedules=all_schedules,
    sensors=all_sensors,
)
