import pipeline
import networkx as nx
from helpers import plot
from config import TABLES_CONFIG, GOLD_TABLES_CONFIG

def create_bronze_func(dataset: str, config: dict):
    """
    Create a function to process bronze data.

    Args:
        dataset (str): The name of the dataset.
        config (dict): Configuration parameters for processing.

    Returns:
        callable: A lambda function that processes bronze data.
    """
    return lambda: pipeline.process_bronze_data(dataset, config)


def create_silver_func(dataset: str, config: dict):
    """
    Create a function to process silver data.

    Args:
        dataset (str): The name of the dataset.
        config (dict): Configuration parameters for processing.

    Returns:
        callable: A lambda function that processes silver data.
    """
    return lambda: pipeline.process_silver_data(dataset, config)


def create_gold_func(dataset: str, config: dict):
    """
    Create a function to process gold data.

    Args:
        dataset (str): The name of the dataset.
        config (dict): Configuration parameters for processing.

    Returns:
        callable: A lambda function that processes gold data.
    """
    return lambda: pipeline.process_gold_data(dataset, config)


def create_dag(plot_dag: bool) -> nx.DiGraph:
    """
    Create a directed acyclic graph (DAG) for processing data.

    Returns:
        nx.DiGraph: A directed graph representing the data processing workflow.
    """
    dag = nx.DiGraph()

    print("Creating nodes and edges for bronze and silver tables")
    for dataset, config in TABLES_CONFIG.items():
        bronze_func = create_bronze_func(dataset, config)
        silver_func = create_silver_func(dataset, config)

        dag.add_node(f"bronze_{dataset}", func=bronze_func)
        dag.add_node(f"silver_{dataset}", func=silver_func)
        dag.add_edge(f"bronze_{dataset}", f"silver_{dataset}")

    print("Creating nodes and edges for gold tables")
    for dataset, config in GOLD_TABLES_CONFIG.items():
        gold_func = create_gold_func(dataset, config)
        dag.add_node(dataset, func=gold_func)

        for dependent in config["depends_on"]:
            dag.add_edge(dependent, dataset)

    # Visualize DAG before running
    if (plot_dag):
        plot.dag(dag)

    return dag


def execute_task(node: dict) -> None:
    """
    Execute a task represented by a node.

    Args:
        node (dict): The node containing the function to execute.

    Returns:
        None
    """
    node["func"]()


def run_dag(dag: nx.DiGraph) -> None:
    """
    Run the DAG by executing tasks in topological order.

    Args:
        dag (nx.DiGraph): The directed acyclic graph to run.

    Returns:
        None
    """
    # Execute tasks in topological order
    for node in nx.topological_sort(dag):
        execute_task(dag.nodes[node])