import matplotlib.pyplot as plt
import networkx as nx

# Visualize DAG with bronze tasks on the left and silver tasks on the right
def dag(dag: nx.DiGraph) -> None:
    """
    Visualize the directed acyclic graph (DAG) with different colors for task types.

    Args:
        dag (nx.DiGraph): The directed acyclic graph to visualize.

    Returns:
        None
    """
    pos = {}
    bronze_y = 0
    silver_y = 0
    gold_y = 0

    for node in dag.nodes:
        if "bronze" in node:
            pos[node] = (0, bronze_y)
            bronze_y -= 1
        elif "silver" in node:
            pos[node] = (1, silver_y)
            silver_y -= 1
        elif "gold" in node or "fct" in node or "dim" in node:
            pos[node] = (2, gold_y)
            gold_y -= 1

    plt.figure(figsize=(12, 8))
    nx.draw(dag, pos, with_labels=True, node_size=3000, node_color="lightblue", font_size=10, font_weight="bold")
    plt.title("DAG Visualization of Data Processing Tasks")
    plt.show()
