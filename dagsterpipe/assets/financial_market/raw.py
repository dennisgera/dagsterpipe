import pandas as pd
from dagster import OpExecutionContext, StaticPartitionsDefinition, asset

from dagsterpipe.resources.alphavantage_resource import AlphaVantageAPI

tickers_partitions_def = StaticPartitionsDefinition(
    ["IBM", "AAPL", "TSLA"]
)

@asset(
    compute_kind="python",
    name="raw_daily_stock_prices",
    description="Raw daily stock prices extracted from AlphaVantage's API",    
    key_prefix=["alphavantage", "time_series"],
    io_manager_key="parquet_s3_io_manager",
    partitions_def=tickers_partitions_def,
)
def raw_daily_stock_prices(context: OpExecutionContext, alphavantage_client: AlphaVantageAPI) -> pd.DataFrame:
    data: dict = alphavantage_client.get_time_series_daily(ticker=context.partition_key)
    df = pd.DataFrame(data)

    return df