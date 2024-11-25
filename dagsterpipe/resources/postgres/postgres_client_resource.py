import abc
import json
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from logging import Logger
from typing import Any, Dict, Optional, Type

import pandas as pd
import psycopg2
import psycopg2.extensions
from dagster import ConfigurableResource, get_dagster_logger
from dagster import _check as check
from pydantic import BaseModel, Field
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.dialects.postgresql import BIGINT, BOOLEAN, FLOAT, JSONB, TIMESTAMP, VARCHAR
from sqlalchemy.exc import SQLAlchemyError


class BasePostgresClient(abc.ABC):
    @abc.abstractmethod
    def execute_query(self, query, fetch_results=False, cursor_factory=None, error_callback=None):
        pass


class PostgresClient(BasePostgresClient):
    def __init__(self, conn_args: Dict[str, Any], autocommit: Optional[bool], log: Logger):
        self.conn_args = conn_args
        self.autocommit = autocommit
        self.log = log

    def execute_query(
        self, query, fetch_results=False, cursor_factory=None, error_callback=None
    ) -> pd.DataFrame | None:
        """Synchronously execute a single query against Postgres and returns a pandas DataFrame.

        Args:
        query (str): The query to execute.
        fetch_results (Optional[bool]): Whether to return the results of executing the query.
            Defaults to False, in which case the query will be executed without retrieving the
            results.
        cursor_factory (Optional[:py:class:`psycopg2.extensions.cursor`]): An alternative
            cursor_factory; defaults to None. Will be used when constructing the cursor.
        error_callback (Optional[Callable[[Exception, Cursor, DagsterLogManager], None]]): A
            callback function, invoked when an exception is encountered during query execution;
            this is intended to support executing additional queries to provide diagnostic
            information, e.g. by querying ``stl_load_errors`` using ``pg_last_copy_id()``. If no
            function is provided, exceptions during query execution will be raised directly.

        Returns:
            Optional[:py:class:`pandas.DataFrame`]: The results of executing the query, if
            ``fetch_results`` is True and the query returns rows.
        """

        check.str_param(query, "query")
        check.bool_param(fetch_results, "fetch_results")
        check.opt_class_param(cursor_factory, "cursor_factory", superclass=psycopg2.extensions.cursor)
        check.opt_callable_param(error_callback, "error_callback")

        with self._get_conn() as conn:
            with self._get_cursor(conn, cursor_factory=cursor_factory) as cursor:
                try:
                    self.log.info(f"Executing query '{query}'")
                    cursor.execute(query)

                    if fetch_results and cursor.rowcount > 0:
                        return pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                    else:
                        self.log.info("Empty result from query")

                except Exception as e:
                    # If autocommit is disabled or not set (it is disabled by default), Redshift
                    # will be in the middle of a transaction at exception time, and because of
                    # the failure the current transaction will not accept any further queries.
                    #
                    # This conn.commit() call closes the open transaction before handing off
                    # control to the error callback, so that the user can issue additional
                    # queries. Notably, for e.g. pg_last_copy_id() to work, it requires you to
                    # use the same conn/cursor, so you have to do this conn.commit() to ensure
                    # things are in a usable state in the error callback.
                    if not self.autocommit:
                        conn.commit()

                    if error_callback is not None:
                        error_callback(e, cursor, self.log)
                    else:
                        raise

    @contextmanager
    def _get_conn(self):
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_args)
            yield conn

        finally:
            if conn:
                conn.close()

    @contextmanager
    def _get_cursor(self, conn, cursor_factory=None):
        check.opt_class_param(cursor_factory, "cursor_factory", superclass=psycopg2.extensions.cursor)

        # Could be none, in which case we should respect the connection default. Otherwise
        # explicitly set to true/false.
        if self.autocommit is not None:
            conn.autocommit = self.autocommit

        with conn:
            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                yield cursor

            # If autocommit is set, we'll commit after each and every query execution. Otherwise, we
            # want to do a final commit after we're wrapped up executing the full set of one or more
            # queries.
            if not self.autocommit:
                conn.commit()


class PersistProcedureEnum(Enum):
    APPEND = "append"
    UPSERT = "upsert"
    REPLACE = "replace"


class PostgresClientResource(ConfigurableResource):
    """
    This resource enables connecting to a Postgres database and issuing queries agains that database.
    """

    host: str = Field()
    port: int = Field(default=5432)
    user: str = Field()
    password: str = Field(default=None)
    database: str = Field()
    autocommit: Optional[bool] = Field(
        default=None,
        description=(
            "Whether to autocommit queries."
            "None by default, which honors the Postgres parameter AUTOCOMMIT. Set to True "
            "or False to enable or disable autocommit mode in the session, respectively."
        ),
    )

    @property
    def logger(self) -> Logger:
        return get_dagster_logger()

    @property
    def conn_args(self) -> Dict[str, Any]:
        return {
            k: getattr(self, k, None)
            for k in (
                "host",
                "port",
                "user",
                "password",
                "database",
                "autocommit",
            )
            if getattr(self, k, None) is not None
        }

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def get_client(self) -> PostgresClient:
        return PostgresClient(self.conn_args, self.autocommit, get_dagster_logger())

    def get_engine(self):
        """
        Creates an SQLAlchemy engine for the PostgreSQL database.
        """
        user = self.conn_args.get("user")
        password = self.conn_args.get("password")
        host = self.conn_args.get("host")
        port = self.conn_args.get("port")
        database = self.conn_args.get("database")
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        return create_engine(connection_string)

    def persist(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        procedure: PersistProcedureEnum,
        primary_key_columns: Optional[list] = None,
        dtype: Optional[dict] = None,
    ) -> None:
        """
        Persist a pandas DataFrame to a Postgres table.

        Args:
            df (pandas.DataFrame): The DataFrame to persist.
            table_name (str): The name of the table to persist to.
            schema (str): The schema of the table to persist to.
            primary_key_columns (list): The primary key columns of the table to persist to.
            procedure (PersistProcedureEnum): The procedure to use when persisting the DataFrame.
            dtype (dict): The data types of the columns of the table to persist to.

        Returns:
            None

        Raises:
            ValueError: If the procedure is not valid.
        """
        engine = self.get_engine()
        client = self.get_client()
        df = df.rename(self._transform_to_sql_column_format, axis="columns").convert_dtypes()
        table_exists = engine.dialect.has_table(engine.connect(), table_name, schema=schema)

        if not isinstance(procedure, PersistProcedureEnum):
            raise ValueError(f"Invalid procedure {procedure}")
        elif procedure == PersistProcedureEnum.UPSERT and table_exists:
            self._sync_table_schema(df, table_name, schema, engine)
            self._upsert_data(df, table_name, schema, primary_key_columns, client)
        else:
            df.to_sql(
                con=engine,
                name=table_name,
                schema=schema,
                if_exists="fail" if procedure.value == "upsert" else procedure.value,
                index=False,
                chunksize=10000,
                method="multi",
                dtype=dtype,
            )

    def _upsert_data(
        self, df: pd.DataFrame, table_name: str, schema: str, primary_key_columns: list, client: PostgresClient
    ) -> None:
        """
        Handle the UPSERT operation for persisting data to a Postgres table.

        Args:
            df (pd.DataFrame): The DataFrame to upsert.
            table_name (str): The name of the table.
            schema (str): The schema of the table.
            primary_key_columns (list): The primary key columns of the table.
            client (PostgresClient): The client to execute the query.

        Returns:
            None
        """

        def format_insert_values(df_batch: pd.DataFrame) -> list:
            return [
                f"({', '.join(self._format_value_for_sql(val) for val in row)})"
                for row in df_batch.itertuples(index=False, name=None)
            ]

        def build_merge_query(df_batch: pd.DataFrame, insert_values: list) -> str:
            conflict_statement = " AND ".join(f'(target."{pk}" = source."{pk}")' for pk in primary_key_columns)
            update_set_clause = ", ".join([f'"{col}" = source."{col}"' for col in df_batch.columns])
            insert_columns_clause = ", ".join([f'"{col}"' for col in df.columns])
            insert_values_clause = ", ".join([f'source."{col}"' for col in df_batch.columns])

            return f"""
                CREATE TEMP TABLE temp_source AS TABLE {schema}.{table_name} WITH NO DATA;
                INSERT INTO temp_source ({insert_columns_clause}) VALUES {', '.join(insert_values)};
                MERGE INTO {schema}.{table_name} AS target
                USING temp_source AS source
                ON {conflict_statement}
                WHEN MATCHED THEN
                    UPDATE SET {update_set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns_clause})
                    VALUES ({insert_values_clause});
            """

        batch_size = 10000
        num_batches = (len(df) + batch_size - 1) // batch_size

        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = start_idx + batch_size
            df_batch = df.iloc[start_idx:end_idx]

            insert_values = format_insert_values(df_batch)
            merge_query = build_merge_query(df_batch, insert_values)
            client.execute_query(merge_query)

    @staticmethod
    def _format_value_for_sql(value) -> str:
        """
        Format a value for SQL query insertion.

        Args:
            value: The value to format.

        Returns:
            str: The formatted value as a string suitable for SQL.
        """
        if isinstance(value, (list, dict)):
            return f"'{json.dumps(value)}'"
        elif pd.isna(value):
            return "NULL"
        elif isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        else:
            return str(value)

    @staticmethod
    def _transform_to_sql_column_format(column) -> str:
        """
        Transform a column name to a format suitable for SQL.

        Args:
            column (str): The column name.

        Returns:
            str: The column name in a format suitable for SQL.
        """
        formatted_name = column.replace(" ", "_").lower()
        formatted_name = "".join(c if c.isalnum() or c == "_" else "_" for c in formatted_name)

        return formatted_name

    def query(
        self, sql_query: str, params: Optional[dict] = None, schema_class: Optional[Type[BaseModel]] = None
    ) -> pd.DataFrame:
        """
        Execute a query against a Postgres database.
        Args:
            query (str): The query to execute.
            params (dict): The parameters to pass to the query.
        Returns:
            pd.DataFrame: The results of the query.
        """
        engine = self.get_engine()
        if params:
            sql_query = sql_query.format(**params)

        df = pd.read_sql(sql_query, engine)
        if schema_class:
            schema_class(rows=df.to_dict("records"))
            return df.loc[:, schema_class.model_fields.keys()]

        return df

    def truncate_table(self, schema: str, table_name: str) -> None:
        """
        Truncate a table in a Postgres database.

        Args:
            schema (str): The schema of the table.
            table_name (str): The name of the table.

        Returns:
            None
        """
        engine = self.get_engine()
        try:
            table_exists = engine.dialect.has_table(engine.connect(), table_name, schema=schema)
            if table_exists:
                with engine.connect() as conn:
                    conn.execute(f"TRUNCATE TABLE {schema}.{table_name}")
                    self.logger.info(f"Truncated table {schema}.{table_name}")
            else:
                self.logger.info(f"Table {schema}.{table_name} does not exist. Skipping truncate.")
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to truncate table {schema}.{table_name}: {e}")
            raise

    def _sync_table_schema(self, df: pd.DataFrame, table_name: str, schema: str, engine) -> None:
        """
        Synchronize the table schema with the DataFrame columns by adding any missing columns.

        Args:
            df (pd.DataFrame): The DataFrame containing new data.
            table_name (str): The name of the table to sync.
            schema (str): The schema of the table.

        Returns:
            None
        """
        metadata = MetaData(schema=schema)
        metadata.reflect(bind=engine)
        table = metadata.tables.get(f"{schema}.{table_name}")

        if table is None:
            return  # No table to sync

        existing_columns = set(table.columns.keys())
        new_columns = set(df.columns) - existing_columns

        if not new_columns:
            return  # No new columns to add

        # Map pandas dtypes to SQLAlchemy types
        dtype_map = {
            "object": VARCHAR(),
            "int64": BIGINT(),
            "float64": FLOAT(),
            "bool": BOOLEAN(),
            "datetime64[ns]": TIMESTAMP(),
            "json": JSONB(),
        }

        with engine.connect() as connection:
            for column in new_columns:
                dtype = str(df[column].dtype)
                # Determine the SQLAlchemy type
                if dtype == "object" and df[column].apply(lambda x: isinstance(x, (dict, list))).any():
                    sqlalchemy_type = JSONB()
                else:
                    sqlalchemy_type = dtype_map.get(dtype, VARCHAR())

                # Construct and execute the ALTER TABLE statement
                alter_query = text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{column}" {sqlalchemy_type};')
                connection.execute(alter_query)
                self.logger.info(f'Added column "{column}" to table "{schema}"."{table_name}".')
