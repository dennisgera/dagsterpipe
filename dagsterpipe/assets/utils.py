from datetime import datetime
from typing import Optional
import pandas as pd
import json

from dagsterpipe.resources.postgres.postgres_pandas_type_handler import (
    convert_numpy_to_json,
)


def to_json_dumps(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if any(isinstance(row, (list, dict)) for row in df[col]):
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False))
    return df


def standardize_data(
    data: list[dict], *, loaded_at: datetime, dedup_columns: Optional[list] = None
) -> pd.DataFrame:
    df = pd.json_normalize(data)
    if dedup_columns:
        df = df.drop_duplicates(subset=dedup_columns)
    return (
        df.dropna(how="all")
        .assign(_dagster_loaded_at=loaded_at)
        .apply(convert_numpy_to_json, axis="index")
        .pipe(to_json_dumps)
    )
