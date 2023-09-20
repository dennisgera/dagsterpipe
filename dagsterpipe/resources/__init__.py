from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from dagsterpipe.resources.alphavantage_resource import AlphaVantageAPI
from dagsterpipe.resources.parquet_s3_io_manager import ConfigurableS3ParquetIOManager


RESOURCES_PROD = {
    "parquet_s3_io_manager": ConfigurableS3ParquetIOManager(
        s3_resource=S3Resource(),
        s3_bucket="dagsterpipe-datalake-raw",
    ),
    "s3_resource": S3Resource(),
    "alphavantage_client": AlphaVantageAPI(
        api_access_key=EnvVar("ALPHAVANTAGE_API_ACCESS_KEY"),
    )
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
    )
}
