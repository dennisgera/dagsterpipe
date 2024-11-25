import json
from typing import Mapping, Optional, Sequence, Type

import numpy as np
import pandas as pd
import pandas.core.dtypes.common as pd_core_dtypes_common
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from dagster._core.definitions.metadata import RawMetadataValue
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from sqlalchemy import text

from dagsterpipe.resources.postgres.dbt_postgres_io_manager import DbtPostgresIOManager
from dagsterpipe.resources.postgres.postgres_io_manager import (
    PostgresDbClient,
    PostgresIOManager,
)


class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def convert_numpy_to_json(s: pd.Series) -> pd.Series:
    """Converts columns of data of type np.ndarray
    to json so that it can be stored in PostgreSQL.
    """
    # check for all values in the series
    s_types = {x for x in s.apply(type)}
    np_types = {np.ndarray, np.generic}
    if any(s_types.intersection(np_types)):
        return s.apply(lambda x: json.dumps(x, cls=NumpyEncoder))
    else:
        return s


def _table_exists(table_slice: TableSlice, connection):
    tables = connection.execute(
        statement=text(
            f"SELECT table_name FROM information_schema.tables WHERE \
                table_name = '{table_slice.table}' AND table_schema = '{table_slice.schema}'"
        )
    ).fetchall()
    return len(tables) > 0


def _get_table_column_types(table_slice: TableSlice, connection) -> Optional[Mapping[str, str]]:
    if _table_exists(table_slice, connection):
        schema_list = connection.execute(
            statement=text(
                f"SELECT column_name, data_type FROM information_schema.columns \
                WHERE table_name = '{table_slice.table}'"
            )
        ).fetchall()
        return {item[0]: item[1] for item in schema_list}


def _convert_timestamp_to_string(s: pd.Series, column_types: Optional[Mapping[str, str]], table_name: str) -> pd.Series:
    """Converts columns of data of type pd.Timestamp to string so that it can be stored in
    PostgreSQL.
    """
    column_name = str(s.name)
    if pd_core_dtypes_common.is_datetime64_dtype(s) or pd_core_dtypes_common.is_timedelta64_dtype(s):
        if column_types:
            if "VARCHAR" not in column_types[column_name]:
                raise DagsterInvariantViolationError(
                    "PostgreSQL I/O manager: PostgreSQL I/O manager configured to convert time data"
                    f" in DataFrame column {column_name} to strings, but the corresponding"
                    f" {column_name.upper()} column in table {table_name} is not of type VARCHAR,"
                    f" it is of type {column_types[column_name]}. Please set"
                    " store_timestamps_as_strings=False in the PostgreSQL I/O manager configuration"
                    " to store time data as TIMESTAMP types."
                )
        return s.dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")
    else:
        return s


def _convert_string_to_timestamp(s: pd.Series) -> pd.Series:
    """Converts columns of strings in Timestamp format to pd.Timestamp to undo the conversion in
    _convert_timestamp_to_string.

    This will not convert non-timestamp strings into timestamps (pd.to_datetime will raise an
    exception if the string cannot be converted)
    """
    if isinstance(s[0], str):
        try:
            return pd.to_datetime(s.values)  # type: ignore  # (bad stubs)
        except ValueError:
            return s
    else:
        return s


def _add_missing_timezone(s: pd.Series, column_types: Optional[Mapping[str, str]], table_name: str) -> pd.Series:
    column_name = str(s.name)
    if pd_core_dtypes_common.is_datetime64_dtype(s) or pd_core_dtypes_common.is_timedelta64_dtype(s):
        if column_types:
            if "VARCHAR" in column_types[column_name]:
                raise DagsterInvariantViolationError(
                    f"PostgreSQL I/O manager: The PostgreSQL column {column_name.upper()} in table"
                    f" {table_name} is of type {column_types[column_name]} and should be of type"
                    f" TIMESTAMP to store the time data in dataframe column {column_name}. Please"
                    " migrate this column to be of time TIMESTAMP_NTZ(9) to store time data."
                )
        return s.dt.tz_localize("UTC")
    return s


class PostgresPandasTypeHandler(DbTypeHandler[pd.DataFrame]):
    """Plugin for the PostgreSQL I/O Manager that can store and load Pandas DataFrames as PostgreSQL tables.

    Examples:
        .. code-block:: python

            from dagster_snowflake import SnowflakeIOManager
            from dagster_snowflake_pandas import SnowflakePandasTypeHandler
            from dagster_snowflake_pyspark import SnowflakePySparkTypeHandler
            from dagster import Definitions, EnvVar

            class MySnowflakeIOManager(SnowflakeIOManager):
                @staticmethod
                def type_handlers() -> Sequence[DbTypeHandler]:
                    return [SnowflakePandasTypeHandler(), SnowflakePySparkTypeHandler()]

            @asset(
                key_prefix=["my_schema"]  # will be used as the schema in snowflake
            )
            def my_table() -> pd.DataFrame:  # the name of the asset will be the table name
                ...

            defs = Definitions(
                assets=[my_table],
                resources={
                    "io_manager": MySnowflakeIOManager(
                                    database="MY_DATABASE",
                                    account=EnvVar("SNOWFLAKE_ACCOUNT"),
                                    ...)
                }
            )
    """

    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection
    ) -> Mapping[str, RawMetadataValue]:
        with_lowercase_cols = obj.rename(str.lower, copy=False, axis="columns")
        column_types = _get_table_column_types(table_slice, connection)
        if context.resource_config and context.resource_config.get("store_timestamps_as_strings", False):
            with_lowercase_cols = with_lowercase_cols.apply(
                lambda x: _convert_timestamp_to_string(x, column_types, table_slice.table),
                axis="index",
            )
        else:
            with_lowercase_cols = with_lowercase_cols.apply(
                lambda x: _add_missing_timezone(x, column_types, table_slice.table), axis="index"
            )
        with_lowercase_cols = with_lowercase_cols.apply(convert_numpy_to_json, axis="index")
        with_lowercase_cols.to_sql(
            table_slice.table,
            con=connection.engine,
            if_exists="append",
            chunksize=10000,
            index=False,
            schema=table_slice.schema,
            dtype=context.metadata.get("dtype_overrides"),
            method=context.metadata.get("to_sql_method"),
        )

        return {
            "row_count": obj.shape[0],
            "dataframe_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[TableColumn(name=str(name), type=str(dtype)) for name, dtype in obj.dtypes.items()]
                )
            ),
        }

    def load_input(self, context: InputContext, table_slice: TableSlice, connection) -> pd.DataFrame:
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pd.DataFrame()
        result = pd.read_sql(sql=PostgresDbClient.get_select_statement(table_slice), con=connection)
        if context.resource_config and context.resource_config.get("store_timestamps_as_strings", False):
            result = result.apply(_convert_string_to_timestamp, axis="index")
        result.columns = map(str.lower, result.columns)  # type: ignore  # (bad stubs)
        return result

    @property
    def supported_types(self):
        return [pd.DataFrame]


class PostgresPandasIOManager(PostgresIOManager):
    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [PostgresPandasTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return pd.DataFrame


class DbtPostgresPandasIOManager(DbtPostgresIOManager):
    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [PostgresPandasTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return pd.DataFrame
