from abc import abstractmethod
from contextlib import contextmanager
from typing import Optional, Sequence, Type, cast

from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from dagsterpipe.resources.postgres.postgres_resource import PostgresResource

POSTGRES_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class PostgresIOManager(ConfigurableIOManagerFactory):
    user: str = Field()
    password: str = Field(default=None)
    database: str = Field()
    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
    )
    port: int = Field(default=5432)
    host: str = Field(default="localhost")

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        return DbIOManager(
            db_client=PostgresDbClient(),
            io_manager_name="PostgresIOManager",
            database=self.database,
            schema=self.schema_,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
        )


class PostgresDbClient(DbClient):
    @staticmethod
    @contextmanager
    def connect(context, table_slice):
        no_schema_config = (
            {k: v for k, v in context.resource_config.items() if k not in ["schema", "dbname"]}
            if context.resource_config
            else {}
        )
        with PostgresResource(**no_schema_config).get_connection(raw_conn=False) as conn:
            yield conn

    @staticmethod
    def ensure_schema_exists(context: OutputContext, table_slice: TableSlice, connection) -> None:
        schemas = connection.execute(
            text(
                "SELECT 1 FROM information_schema.schemata"
                f" WHERE schema_name = '{table_slice.schema}' and catalog_name = '{table_slice.database}'"
            )
        ).fetchall()
        if len(schemas) == 0:
            connection.execute(text(f"create schema {table_slice.schema};"))

    @staticmethod
    def delete_table_slice(context: OutputContext, table_slice: TableSlice, connection) -> None:
        try:
            connection.execute(text(_get_cleanup_statement(table_slice)))
        except ProgrammingError:
            # table doesn't exist yet, so ignore the error
            pass

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"
        if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
            query = f"SELECT {col_str} FROM" f" {table_slice.database}.{table_slice.schema}.{table_slice.table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)
        else:
            return f"""SELECT {col_str} FROM {table_slice.database}.{table_slice.schema}.{table_slice.table}"""


def _get_cleanup_statement(table_slice: TableSlice) -> str:
    """Returns a SQL statement that deletes data in the given table to make way for the output data
    being written.
    """
    if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
        query = f"DELETE FROM {table_slice.database}.{table_slice.schema}.{table_slice.table} WHERE\n"
        return query + _partition_where_clause(table_slice.partition_dimensions)
    else:
        return f"DELETE FROM {table_slice.database}.{table_slice.schema}.{table_slice.table}"


def _partition_where_clause(partition_dimensions: Sequence[TablePartitionDimension]) -> str:
    return " AND\n".join(
        _time_window_where_clause(partition_dimension)
        if isinstance(partition_dimension.partitions, TimeWindow)
        else _static_where_clause(partition_dimension)
        for partition_dimension in partition_dimensions
    )


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.strftime(POSTGRES_DATETIME_FORMAT)
    end_dt_str = end_dt.strftime(POSTGRES_DATETIME_FORMAT)
    return (
        f"{table_partition.partition_expr} >= '{start_dt_str}'"
        f" AND {table_partition.partition_expr} < '{end_dt_str}'"
    )


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""
