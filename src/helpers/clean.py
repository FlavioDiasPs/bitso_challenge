import pandas as pd
import pyarrow as pa
from typing import List

def standardize_datetime_format(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Standardize the datetime format in the specified column to microseconds precision.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        column_name (str): The name of the column to standardize.

    Returns:
        pd.DataFrame: The DataFrame with the standardized datetime column.
    """
    df[column_name] = pd.to_datetime(df[column_name], errors="coerce", utc=True)
    df[column_name] = df[column_name].dt.floor("us")
    return df


def convert_df_to_schema(df: pd.DataFrame, schema: List[pa.Field]) -> pd.DataFrame:
    """
    Convert DataFrame columns to match the specified schema.

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        schema (List[pa.Field]): The schema defining the desired column types.

    Returns:
        pd.DataFrame: The DataFrame with columns converted to the specified schema.
    """
    for field in schema:
        column_name = field.name
        column_type = field.type

        if pa.types.is_int64(column_type):
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce").astype("Int64")
        elif pa.types.is_int32(column_type):
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce").astype("Int32")
        elif pa.types.is_timestamp(column_type):
            df = standardize_datetime_format(df, column_name)
            df[column_name] = df[column_name].astype("datetime64[us, UTC]")  # Convert to microseconds with timezone
        elif pa.types.is_string(column_type):
            df[column_name] = df[column_name].fillna("").astype("string")
        elif pa.types.is_float64(column_type):
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0.0).astype("float64")
        else:
            raise ValueError(f"Unsupported type: {column_type}")

    return df


def drop_duplicates(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """
    Drop duplicate rows from the DataFrame based on specified keys.

    Args:
        df (pd.DataFrame): The DataFrame from which to drop duplicates.
        keys (List[str]): The column names to consider for identifying duplicates.

    Returns:
        pd.DataFrame: The DataFrame after duplicates have been removed.
    """
    before_dedup = len(df)
    print(f"Amount of rows before dedup: {before_dedup}")

    df.drop_duplicates(subset=keys, inplace=True, ignore_index=True)

    after_dedup = len(df)
    print(f"Amount of rows after dedup: {after_dedup}")
    print(f"Amount of rows deduplicated: {before_dedup - after_dedup}")

    return df
