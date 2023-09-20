from dagster import define_asset_job, in_process_executor

from dagsterpipe.assets.financial_market.raw import raw_daily_stock_prices


sync_daily_stock_prices = define_asset_job(
    name="sync_daily_stock_prices",
    description=("Get daily stock prices using AlphaVantage API and save it to S3 bucket"),
    selection=[
        raw_daily_stock_prices
    ],
    executor_def=in_process_executor,
)