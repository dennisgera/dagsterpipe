from datetime import datetime, timezone
import pandas as pd
from dagster import OpExecutionContext, StaticPartitionsDefinition, asset, MaterializeResult

from dagsterpipe.resources.alphavantage_resource import AlphaVantageAPI
from dagsterpipe.resources.postgres.postgres_client_resource import (
    PersistProcedureEnum,
    PostgresClientResource,
)

tickers_partitions_def = StaticPartitionsDefinition(["IBM", "AAPL", "TSLA", "MSFT", "GOOGL"])

def process_time_series_daily(data: dict, loaded_at: datetime) -> pd.DataFrame:
    """
    Process Alpha Vantage daily time series data into a pandas DataFrame.
    
    Args:
        data: Raw Alpha Vantage API response dictionary
        
    Returns:
        DataFrame with processed time series data including metadata
    """
    meta = {
        k.split(". ")[1].lower(): v 
        for k, v in data.get("Meta Data", {}).items()
    }

    df = (
        pd.DataFrame.from_dict(data.get("Time Series (Daily)", {}), orient="index")
        .rename(columns=lambda x: x.split(". ")[1].lower())
        .rename_axis("date")
        .reset_index()
    )
    
    numeric_columns = ["open", "high", "low", "close", "volume"]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")
    
    df = df.assign(
        symbol=meta.get("symbol"),
        last_refreshed=meta.get("last refreshed"),
        loaded_at=loaded_at,
    )

    return df


@asset(
    compute_kind="python",
    name="daily_stock_prices",
    description="Raw daily stock prices extracted from AlphaVantage's API",
    key_prefix=["raw", "raw_alphavantage"],
    partitions_def=tickers_partitions_def,
)
def daily_stock_prices(
    context: OpExecutionContext,
    alphavantage_client: AlphaVantageAPI,
    postgres_database: PostgresClientResource,
) -> MaterializeResult:
    loaded_at = datetime.now(tz=timezone.utc)
    data: dict = alphavantage_client.get_time_series_daily(ticker=context.partition_key)

    df = process_time_series_daily(data=data, loaded_at=loaded_at)

    postgres_database.persist(
        df=df,
        table_name="daily_stock_prices",
        schema="raw_alphavantage",
        procedure=PersistProcedureEnum.UPSERT,
        primary_key_columns=["symbol", "date"],
    )

    return MaterializeResult(asset_key=context.asset_key)
    
