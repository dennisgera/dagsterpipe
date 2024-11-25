import os
from dagster import EnvVar, file_relative_path
from dagster_aws.s3 import S3Resource
from dagsterpipe.resources.alphavantage_resource import AlphaVantageAPI
from dagsterpipe.resources.parquet_s3_io_manager import ConfigurableS3ParquetIOManager
from dagster_dbt import DbtCliResource

from dagsterpipe.resources.postgres.postgres_client_resource import PostgresClientResource
from dagsterpipe.resources.postgres.postgres_pandas_type_handler import DbtPostgresPandasIOManager, PostgresPandasIOManager

DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt")
USER = os.getenv("USER", "john_doe")


RESOURCES_PROD = {
    "parquet_s3_io_manager": ConfigurableS3ParquetIOManager(
        s3_resource=S3Resource(),
        s3_bucket="dagsterpipe-datalake-raw",
    ),
    "s3_resource": S3Resource(),
    "alphavantage_client": AlphaVantageAPI(
        api_access_key=EnvVar("ALPHAVANTAGE_API_ACCESS_KEY"),
    ),
    "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, target="prod"),
    "dbt_postgres_pandas_io_manager": DbtPostgresPandasIOManager(
        database="warehouse",
        host=EnvVar("WAREHOUSE_HOST"),
        port=5432,
        user="dbt",
        password=EnvVar("DBT_PASSWORD"),
        schema="analytics",
    ),
    "postgres_database": PostgresClientResource(
        database="warehouse",
        host=EnvVar("WAREHOUSE_HOST"),
        port=5432,
        user="dagster",
        password=EnvVar("DAGSTER_RW_PASS"),
    ),
    "postgres_pandas_io_manager": PostgresPandasIOManager(
        database="warehouse",
        host=EnvVar("WAREHOUSE_HOST"),
        port=5432,
        user="dagster",
        password=EnvVar("DAGSTER_RW_PASS"),
    ),
}


RESOURCES_LOCAL = {    
    "parquet_s3_io_manager": ConfigurableS3ParquetIOManager(
        s3_resource=S3Resource(
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        ),
        s3_bucket="local-warehouse",
    ),
    "s3_resource": S3Resource(
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    ),
    "alphavantage_client": AlphaVantageAPI(
        api_access_key=EnvVar("ALPHAVANTAGE_API_ACCESS_KEY"),
    ),
    "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, target="dev"),
    "dbt_postgres_pandas_io_manager": DbtPostgresPandasIOManager(
        database="warehouse",
        host=EnvVar("WAREHOUSE_HOST"),
        port=5432,
        user="dbt_dev",
        password=EnvVar("DBT_PASSWORD"),
        schema=USER,
    ),
    "postgres_database": PostgresClientResource(
        database="warehouse",
        host="localhost",
        port=5432,
        user="postgres",
    ),
    "postgres_pandas_io_manager": PostgresPandasIOManager(
        database="warehouse",
        host="localhost",
        port=5432,
        user="postgres",
    ),
}
