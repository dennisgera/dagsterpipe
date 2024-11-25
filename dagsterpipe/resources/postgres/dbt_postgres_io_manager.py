from abc import abstractmethod
from typing import List, Mapping, Optional, Sequence, Type, Union, cast

from dagster import (
    DagsterInvalidDefinitionError,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.db_io_manager import (
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field

from dagsterpipe.resources.postgres.postgres_io_manager import PostgresDbClient

POSTGRES_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class DbtDBIoManager(DbIOManager):
    def _get_table_slice(
        self, context: Union[OutputContext, InputContext], output_context: OutputContext
    ) -> TableSlice:
        output_context_metadata = output_context.metadata or {}

        schema: str
        table: str
        partition_dimensions: List[TablePartitionDimension] = []

        if not self._schema:
            raise ValueError("Schema must be specified in DbtIOManager resource config")

        if context.has_asset_key:
            asset_key_path = context.asset_key.path
            table = asset_key_path[-1]

            if len(asset_key_path) > 1 and self._schema:
                schema = f"{self._schema}_{asset_key_path[-2]}"
            else:
                schema = self._schema

            if context.has_asset_partitions:
                partition_expr = output_context_metadata.get("partition_expr")
                if partition_expr is None:
                    raise ValueError(
                        f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                        " metadata value, so we don't know what column it's partitioned on. To"
                        " specify a column, set this metadata value. E.g."
                        ' @asset(metadata={"partition_expr": "your_partition_column"}).'
                    )

                if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
                    multi_partition_key_mapping = cast(MultiPartitionKey, context.asset_partition_key).keys_by_dimension
                    for part in context.asset_partitions_def.partitions_defs:
                        partition_key = multi_partition_key_mapping[part.name]
                        if isinstance(part.partitions_def, TimeWindowPartitionsDefinition):
                            partitions = part.partitions_def.time_window_for_partition_key(partition_key)
                        else:
                            partitions = [partition_key]

                        partition_expr_str = cast(Mapping[str, str], partition_expr).get(part.name)
                        if partition_expr is None:
                            raise ValueError(
                                f"Asset '{context.asset_key}' has partition {part.name}, but the"
                                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                                " so we don't know what column to filter it on. Specify which"
                                " column of the database contains data for the"
                                f" {part.name} partition."
                            )
                        partition_dimensions.append(
                            TablePartitionDimension(partition_expr=cast(str, partition_expr_str), partitions=partitions)
                        )
                elif isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=context.asset_partitions_time_window if context.asset_partition_keys else [],
                        )
                    )
                else:
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=context.asset_partition_keys,
                        )
                    )
        else:
            table = output_context.name
            if output_context_metadata.get("schema") and self._schema:
                raise DagsterInvalidDefinitionError(
                    f"Schema {output_context_metadata.get('schema')} "
                    "specified via output metadata, but conflicting schema "
                    f"{self._schema} was provided via run_config. "
                    "Schema can only be specified one way."
                )
            elif output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            else:
                schema = "public"

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.metadata or {}).get("columns"),
        )


class DbtPostgresIOManager(ConfigurableIOManagerFactory):
    user: str = Field()
    password: str = Field()
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

    def create_io_manager(self, context) -> DbtDBIoManager:
        return DbtDBIoManager(
            db_client=PostgresDbClient(),
            io_manager_name="DbtPostgresIOManager",
            database=self.database,
            schema=self.schema_,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
        )
