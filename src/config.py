import pyarrow as pa
from dal.queries import (
    populate_fact_transactions,
    populate_scd_dim_entity_duckdb,
)

DATABASE_SQLITE = {"database": "data/bitso.db"}
DATABASE_DUCKDB = {"database": ":memory:"}

TABLES_CONFIG = {
    "deposit": {
        "raw_path": "data/raw/deposit_sample_data.csv",
        "bronze_path": "data/delta/bronze/bronze_deposit",
        "silver_path": "data/delta/silver/silver_deposit",
        "primary_keys": ["id"],
        "schema": pa.schema(
            [
                ("id", pa.int64()),
                ("event_timestamp", pa.timestamp("us", tz="UTC")),
                ("user_id", pa.string()),
                ("amount", pa.float64()),
                ("currency", pa.string()),
                ("tx_status", pa.string()),
            ]
        ),
    },
    "event": {
        "raw_path": "data/raw/event_sample_data.csv",
        "bronze_path": "data/delta/bronze/bronze_event",
        "silver_path": "data/delta/silver/silver_event",
        "primary_keys": ["id"],
        "schema": pa.schema(
            [
                ("id", pa.int64()),
                ("event_timestamp", pa.timestamp("us", tz="UTC")),
                ("user_id", pa.string()),
                ("event_name", pa.string()),
            ]
        ),
    },
    "user_id": {
        "raw_path": "data/raw/user_id_sample_data.csv",
        "bronze_path": "data/delta/bronze/bronze_user_id",
        "silver_path": "data/delta/silver/silver_user_id",
        "primary_keys": ["user_id"],
        "schema": pa.schema(
            [
                ("user_id", pa.string()),
            ]
        ),
    },
    "user_level": {
        "raw_path": "data/raw/user_level_sample_data.csv",
        "bronze_path": "data/delta/bronze/bronze_user_level",
        "silver_path": "data/delta/silver/silver_user_level",
        "primary_keys": ["user_id", "event_timestamp"],
        "schema": pa.schema(
            [
                ("user_id", pa.string()),
                ("jurisdiction", pa.string()),
                ("level", pa.int32()),
                ("event_timestamp", pa.timestamp("us", tz="UTC")),
            ]
        ),
    },
    "withdrawals": {
        "raw_path": "data/raw/withdrawals_sample_data.csv",
        "bronze_path": "data/delta/bronze/bronze_withdrawals",
        "silver_path": "data/delta/silver/silver_withdrawals",
        "primary_keys": ["id"],
        "schema": pa.schema(
            [
                ("id", pa.int64()),
                ("event_timestamp", pa.timestamp("us", tz="UTC")),
                ("user_id", pa.string()),
                ("amount", pa.float64()),
                ("interface", pa.string()),
                ("currency", pa.string()),
                ("tx_status", pa.string()),
            ]
        ),
    },
}

GOLD_TABLES_CONFIG = {
    "fact_transactions": {
        "gold_path": "data/delta/gold/fact_transactions",
        "depends_on": ["silver_deposit", "silver_withdrawals"],
        "primary_keys": ["transaction_id", "transaction_type"],
        "populate_query": populate_fact_transactions.query,
    },
    "dim_entity": {
        "gold_path": "data/delta/gold/dim_entity",
        "depends_on": ["silver_user_id", "silver_user_level"],
        "primary_keys": ["entity_id"],
        "populate_query": populate_scd_dim_entity_duckdb.query,
    },
    "dim_date": {
        "gold_path": "data/delta/gold/dim_date",
        "depends_on": [],
        "primary_keys": ["full_date"],
        "populate_query": "",
    },
}
