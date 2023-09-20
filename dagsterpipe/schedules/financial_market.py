from dagster import DefaultScheduleStatus, ScheduleDefinition

from dagsterpipe.jobs.financial_market.sync import sync_daily_stock_prices
from dagsterpipe.config import TZ


sync_daily_stock_prices_schedule = ScheduleDefinition(
    name="sync_daily_stock_prices_schedule",
    cron_schedule="0 1 * * *",
    job=sync_daily_stock_prices,
    execution_timezone=TZ,
    default_status=DefaultScheduleStatus.RUNNING,
)
