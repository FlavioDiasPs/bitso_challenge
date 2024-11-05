import duckdb

def create_connection() -> duckdb.DuckDBPyConnection:
    """
    Create a connection to an in-memory DuckDB database.

    Returns:
        duckdb.DuckDBPyConnection: The connection object to the DuckDB database.
    """
    return duckdb.connect(":memory:")

def create_delta_table_in_memory(dataset: str, path: str, con: duckdb.DuckDBPyConnection) -> None:
    """
    Create a Delta table in memory from a specified Delta Lake path.

    Args:
        dataset (str): The name of the dataset (table) to create.
        path (str): The path to the Delta Lake table.
        con (duckdb.DuckDBPyConnection): The DuckDB connection object.

    Returns:
        None
    """
    con.execute(
        f"""
        CREATE TABLE {dataset} AS
        SELECT * FROM delta_scan('{path}') 
        """
    )
