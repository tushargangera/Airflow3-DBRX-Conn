from datetime import datetime
from airflow import DAG
from airflow.models import Connection
from airflow.settings import Session
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Define Connection Parameters
CONN_ID = "databricks_mwaa_conn"
DATABRICKS_HOST = "https://adb-123456789.cloud.databricks.com"  # Replace with your workspace URL
DATABRICKS_TOKEN = "dapi1234567890abcdef"                       # Replace with your Personal Access Token (PAT)

def create_databricks_connection():
    """Programmatically checks and creates a Databricks connection in MWAA database."""
    session = Session()
    try:
        # Check if connection already exists to avoid duplicates
        existing_conn = session.query(Connection).filter(Connection.conn_id == CONN_ID).first()
        
        if not existing_conn:
            new_conn = Connection(
                conn_id=CONN_ID,
                conn_type="databricks",
                host=DATABRICKS_HOST,
                password=DATABRICKS_TOKEN
            )
            session.add(new_conn)
            session.commit()
            print(f"Successfully created connection: {CONN_ID}")
        else:
            print(f"Connection {CONN_ID} already exists.")
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="databricks_connection_via_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["databricks", "mwaa"],
) as dag:

    # Task 1: Initialize the connection inside the Airflow metadata database
    init_connection = PythonOperator(
        task_id="init_databricks_connection",
        python_callable=create_databricks_connection,
    )

    # Task 2: Use the created connection in a Databricks Operator
    # This example submits a notebook run on an on-demand cluster
    run_notebook = DatabricksSubmitRunOperator(
        task_id="submit_databricks_job",
        databricks_conn_id=CONN_ID,
        json={
            "tasks": [
                {
                    "task_key": "mwaa_notebook_task",
                    "new_cluster": {
                        "spark_version": "14.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 1,
                    },
                    "notebook_task": {
                        "notebook_path": "/Users/your_user@://domain.com",
                    },
                }
            ]
        },
    )

    # Set dependency: connection must be verified/created before running the job
    init_connection >> run_notebook
