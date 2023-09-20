from dagster import (
    load_assets_from_package_module,
)

from dagsterpipe.assets import financial_market

FINANCIAL_MARKET = "financial_market"

financial_market_assets = load_assets_from_package_module(
    package_module=financial_market,
    group_name=FINANCIAL_MARKET,
)
