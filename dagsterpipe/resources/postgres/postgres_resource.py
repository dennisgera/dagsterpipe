from contextlib import contextmanager
from typing import Any, Dict, Iterator, Mapping, Optional

from dagster import ConfigurableResource
from dagster._annotations import public
from dagster._core.storage.event_log.sql_event_log import SqlDbConnection
from dagster._utils.cached_method import cached_method
from pydantic import Field, validator


class PostgresResource(ConfigurableResource):
    user: str = Field()
    database: str = Field()
    password: str = Field(default=None)
    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
    )
    port: int = Field(default=5432)
    host: str = Field(default="localhost")
    autocommit: Optional[bool] = Field(
        default=None,
        description=(
            "None by default, which honors the Postgres parameter AUTOCOMMIT. Set to True "
            "or False to enable or disable autocommit mode in the session, respectively."
        ),
    )

    validate_default_parameters: Optional[bool] = Field(
        default=None,
        description=(
            "If True, raise an exception if the warehouse, database, or schema doesn't exist." " Defaults to False."
        ),
    )

    paramstyle: Optional[str] = Field(
        default=None,
        description=(
            "pyformat by default for client side binding. Specify qmark or numeric to "
            "change bind variable formats for server side binding."
        ),
    )

    timezone: Optional[str] = Field(
        default=None,
    )

    connector: Optional[str] = Field(
        default="sqlalchemy",
    )

    @validator("paramstyle")
    def validate_paramstyle(cls, v: Optional[str]) -> Optional[str]:
        valid_config = ["pyformat", "qmark", "numeric"]
        if v is not None and v not in valid_config:
            raise ValueError(
                "Postgres Resource: 'paramstyle' configuration value must be one of:" f" {','.join(valid_config)}."
            )
        return v

    @validator("connector")
    def validate_connector(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v != "sqlalchemy":
            raise ValueError("Postgres Resource: 'connector' configuration value must be None or sqlalchemy.")
        return v

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @property
    @cached_method
    def _connection_args(self) -> Mapping[str, Any]:
        conn_args = {
            k: self._resolved_config_dict.get(k)
            for k in (
                "user",
                "password",
                "database",
                "schema",
                "host",
                "port",
                "autocommit",
                "validate_default_parameters",
                "paramstyle",
                "timezone",
            )
            if self._resolved_config_dict.get(k) is not None
        }
        return conn_args

    @property
    @cached_method
    def _sqlalchemy_connection_args(self) -> Mapping[str, Any]:
        conn_args: Dict[str, Any] = {
            k: self._resolved_config_dict.get(k)
            for k in (
                "host",
                "user",
                "password",
                "database",
                "schema",
                "port",
            )
            if self._resolved_config_dict.get(k) is not None
        }

        return conn_args

    @property
    @cached_method
    def _sqlalchemy_engine_args(self) -> Mapping[str, Any]:
        sqlalchemy_engine_args = self._resolved_config_dict
        return sqlalchemy_engine_args

    @public
    @contextmanager
    def get_connection(self, raw_conn: bool = True) -> Iterator[SqlDbConnection]:
        from sqlalchemy import create_engine

        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}",
            connect_args=self._sqlalchemy_connection_args,
        )
        conn = engine.raw_connection() if raw_conn else engine.connect()

        yield conn
        conn.close()
        engine.dispose()
