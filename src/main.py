
import schedule
import time
import dag_manager


if __name__ == "__main__":
    dag = dag_manager.create_dag(plot_dag=False)

    dag_manager.run_dag(dag)
    print("Leaving.")

    # Schedule the processes
    # interval = 1
    # schedule.every(interval).seconds.do(run_dag, dag)

    # # Orchestrator loop
    # while True:
    #     schedule.run_pending()
    #     break