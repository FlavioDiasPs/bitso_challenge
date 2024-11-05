import os
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from dal import my_sqlite, my_duckdb
from helpers import clean
from types import SimpleNamespace
from dal.queries import create_all_gold_tables


def process_bronze_data(dataset: str, config) -> None:
    """
    Process raw data and save it to bronze format.

    Args:
        dataset (str): The name of the dataset being processed.
        config (dict): Configuration parameters including paths for raw and bronze data.

    Returns:
        None
    """
    config = SimpleNamespace(**config)

    print(f"\n\n ### Starting raw -> bronze task for {dataset} ### ")

    # df = pd.read_csv(config.raw_path)
    # write_deltalake(config.bronze_path, df, mode="overwrite")

    # print(f"Saving {dataset} to bronze CSV")
    # csv_path = f"data/csv/bronze/bronze_{dataset}.csv"
    # os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    # df.to_csv(csv_path, index=False)

    # print(f"{dataset} has been successfully saved")

    # # Temporary save to SQLite
    # my_sqlite.save_replace(df, f"bronze_{dataset}")


def process_silver_data(dataset: str, config) -> None:
    """
    Process bronze data and save it to silver format.

    Args:
        dataset (str): The name of the dataset being processed.
        config (dict): Configuration parameters including paths for bronze and silver data.

    Returns:
        None
    """
    config = SimpleNamespace(**config)

    print(f"\n\n ### Starting bronze -> silver task for {dataset} ### ")
    # df = DeltaTable(config.bronze_path).to_pandas()

    # print(f"Applying schema to {dataset}")
    # df = clean.convert_df_to_schema(df, config.schema)

    # print(f"Deduplicating {dataset} based on {config.primary_keys}")
    # df = clean.drop_duplicates(df, config.primary_keys)

    # print(f"Saving {dataset} to silver Delta Lake")
    # write_deltalake(config.silver_path, df, schema=config.schema, mode="overwrite")

    # print(f"Saving {dataset} to silver CSV")
    # csv_path = f"data/csv/silver/silver_{dataset}.csv"
    # os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    # df.to_csv(csv_path, index=False)

    # # Temporary save to SQLite
    # my_sqlite.save_replace(df, f"silver_{dataset}")


def process_gold_data(dataset: str, config) -> None:
    """
    Process silver data and save it to gold format.

    Args:
        dataset (str): The name of the dataset being processed.
        config (dict): Configuration parameters including paths for silver and gold data.

    Returns:
        None
    """
    config = SimpleNamespace(**config)
    silver_path = 'data/delta/silver/'

    if not config.populate_query:
        return

    print(f"\n\n ### Starting silver -> gold task for {dataset} ### ")
    with my_duckdb.create_connection() as con:
        print("Creating empty tables in-memory")
        con.execute(create_all_gold_tables.query)

        for dependent in config.depends_on:
            print(f"Creating {silver_path}{dependent} in-memory table to create {dataset}")
            my_duckdb.create_delta_table_in_memory(dependent, f"{silver_path}{dependent}", con)

        print(f"Populating {dataset}")
        added = con.execute(config.populate_query).df()
        print(f"Rows Affected: {added} at {dataset}")

        df = con.execute(f"SELECT * FROM {dataset}").df()
        print(df)

        print(f"Saving {dataset} to gold CSV")
        csv_path = f"data/csv/gold/{dataset}.csv"
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path, index=False)

        # Temporary save to SQLite
        my_sqlite.save_replace(df, dataset)

        print(f"Saving {dataset} as gold Delta table")
        write_deltalake(config.gold_path, df, mode="overwrite")
