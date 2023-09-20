from io import BytesIO
from typing import Any, Dict, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    ResourceDependency,
    UPathIOManager,
)
from dagster import _check as check
from dagster._utils.cached_method import cached_method
from dagster_aws.s3 import S3Resource
from pydantic import Field
from upath import UPath


class S3ParquetIOManager(UPathIOManager):
    def __init__(
        self,
        s3_bucket: str,
        s3_session: Any,
        s3_prefix: Optional[str] = None,
        extension: Optional[str] = None,
    ):
        self.bucket = check.str_param(s3_bucket, "s3_bucket")
        check.opt_str_param(s3_prefix, "s3_prefix")
        self.s3 = s3_session
        self.s3.list_objects(Bucket=s3_bucket, Prefix=s3_prefix, MaxKeys=1)
        self.extension = check.opt_str_param(extension, "extension")
        base_path = UPath(s3_prefix) if s3_prefix else None
        super().__init__(base_path=base_path)

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        obj = self.s3.get_object(Bucket=self.bucket, Key=str(path))["Body"].read()
        return pd.read_parquet(BytesIO(obj))

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        if self.path_exists(path):
            context.log.warning(f"Removing existing S3 object: {path}")
            self.unlink(path)

        if isinstance(obj, pd.DataFrame):
            table = pa.Table.from_pandas(obj)
            buf = BytesIO()
            pq.write_table(table, buf)
            self.s3.put_object(Bucket=self.bucket, Key=str(path), Body=buf.getvalue())
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

    def path_exists(self, path: UPath) -> bool:
        try:
            self.s3.get_object(Bucket=self.bucket, Key=str(path))
        except self.s3.exceptions.NoSuchKey:
            return False
        return True

    def get_loading_input_log_message(self, path: UPath) -> str:
        return f"Loading S3 object from: {self._uri_for_path(path)}"

    def get_writing_output_log_message(self, path: UPath) -> str:
        return f"Writing S3 object at: {self._uri_for_path(path)}"

    def unlink(self, path: UPath) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=str(path))

    def make_directory(self, path: UPath) -> None:
        # It is not necessary to create directories in S3
        return None

    def get_metadata(self, context: OutputContext, obj: Any) -> Dict[str, MetadataValue]:
        path = self._get_path(context)
        return {"uri": MetadataValue.path(self._uri_for_path(path))}

    def get_op_output_relative_path(self, context: Union[InputContext, OutputContext]) -> UPath:
        return UPath("storage", super().get_op_output_relative_path(context))

    def _uri_for_path(self, path: UPath) -> str:
        return f"s3://{self.bucket}/{path}"

    def _with_extension(self, path: UPath) -> UPath:
        return UPath(f"{path}/raw{self.extension}") if self.extension else path


class ConfigurableS3ParquetIOManager(ConfigurableIOManager):
    s3_resource: ResourceDependency[S3Resource]
    s3_bucket: str = Field(description="S3 bucket to use for the file manager.")
    s3_prefix: str = Field(default="dagster", description="Prefix to use for the S3 bucket for this file manager.")
    extension: str = Field(default=".parquet", description="Extension to use for the file manager.")

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    @cached_method
    def inner_io_manager(self) -> S3ParquetIOManager:
        return S3ParquetIOManager(
            s3_bucket=self.s3_bucket,
            s3_session=self.s3_resource.get_client(),
            s3_prefix=self.s3_prefix,
            extension=self.extension,
        )

    def load_input(self, context: InputContext) -> Any:
        return self.inner_io_manager().load_input(context)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        return self.inner_io_manager().handle_output(context, obj)
