from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from config import DATABASE_SQLITE
import pandas as pd

def create_sqlite_engine() -> Engine:
    """
    Create a SQLAlchemy engine for the SQLite database.

    Returns:
        Engine: The SQLAlchemy engine connected to the SQLite database.
    """
    return create_engine(f"sqlite:///{DATABASE_SQLITE['database']}")

def save_replace(df: pd.DataFrame, dataset: str) -> None:
    """
    Save a DataFrame to the SQLite database, replacing the existing table if it exists.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        dataset (str): The name of the dataset (table) in the database.

    Returns:
        None
    """
    engine = create_sqlite_engine()
    with engine.begin() as connection:
        df.to_sql(dataset, con=connection, if_exists="replace", index=False)

def update(dataset: str, watermark: str) -> None:
    """
    Update the watermark for a specified dataset in the database.

    Args:
        dataset (str): The name of the dataset.
        watermark (str): The new watermark value.

    Returns:
        None
    """
    engine = create_sqlite_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE watermark 
                SET watermark = :watermark
                WHERE dataset = :dataset
                """
            ),
            {"watermark": watermark, "dataset": dataset},
        )

def read(dataset: str) -> str | None:
    """
    Read the watermark for a specified dataset from the database.

    Args:
        dataset (str): The name of the dataset.

    Returns:
        str or None: The watermark value if found, or None if not found or if it was just initialized.
    """
    engine = create_sqlite_engine()

    with engine.connect() as conn:
        watermark = conn.execute(
            text(
                """
                SELECT watermark
                FROM watermark
                WHERE dataset = :dataset
                """
            ),
            {"dataset": dataset},
        ).fetchone()

        if watermark is None:
            first_watermark(dataset)
            return None

        return watermark[0]

def first_watermark(dataset: str) -> None:
    """
    Initialize the watermark for a specified dataset in the database.

    Args:
        dataset (str): The name of the dataset.

    Returns:
        None
    """
    engine = create_sqlite_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO watermark (dataset, watermark) VALUES (:dataset, :watermark)
                """
            ),
            {"dataset": dataset, "watermark": None},
        )

def create_table() -> None:
    """
    Create the watermark table in the SQLite database if it does not exist.

    Returns:
        None
    """
    engine = create_sqlite_engine()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS watermark (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset TEXT,
                    watermark DATETIME
                );
                """
            )
        )
