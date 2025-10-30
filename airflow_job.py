from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 10, 29),
}

# Define the DAG
with DAG(
    dag_id="customer_sales_dataproc_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Define GCS Bucket & File Pattern
    gcs_bucket = "customer-sales-data-bucket1"
    sales_data_prefix = "sales_data/customer_sales_pipeline_dataset.csv"
    spark_job_path = "spark_job/customer_sales_spark_job.py"

    # Task 1: GCS Sensor (Detects Latest JSON File)
    file_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_csv_file_arrival",
        bucket=gcs_bucket,
        prefix=sales_data_prefix,
        timeout=600,
        poke_interval=30,
        mode="poke",
    )

    # Generate a unique batch ID using UUID
    batch_id = f"customer-sales-batch-{str(uuid.uuid4())[:8]}"  # Shortened UUID

    # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://customer-sales-data-bucket1/spark_job/customer_sales_spark_job.py"
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": "github-actions-deployer@nodal-height-473719-u3.iam.gserviceaccount.com",
                "network_uri": "projects/nodal-height-473719-u3/global/networks/default",
                "subnetwork_uri": "projects/nodal-height-473719-u3/regions/us-central1/subnetworks/default",
            }
        },
    }

    run_spark_job = DataprocCreateBatchOperator(
        task_id="run_spark_job",
        batch=batch_details,
        batch_id=batch_id,
        project_id="nodal-height-473719-u3",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )


      # Define task order
    file_sensor >> run_spark_job