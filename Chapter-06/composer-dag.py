import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = "YOUR PROJECT ID" # REPLACE YOUR PROJECT ID
REGION = "us-central1" # Repace this with your preferred region
ZONE = "us-central1-a" # Repace this with your preferred zone
SERVICE_ACCOUNT_EMAIL = "data-processing-sa@YOUR PROJECT ID.iam.gserviceaccount.com" # REPLACE WITH YOUR SERVICE ACCOUNT THAT YOU CREATED (the project id is likely to be the only part that needs to be changed)
PYSPARK_URI = "gs://GCS-BUCKET-NAME/code/chapter-6-pyspark.py" # REPLACE WITH YOUR BUCKET NAME AND FILE PATH
CLUSTER_NAME =  "composer-dp-cluster"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {'start_date': YESTERDAY}

# Cluster definition

CLUSTER_CONFIG = {
   "gce_cluster_config": {"service_account": SERVICE_ACCOUNT_EMAIL},
   "master_config": {
       "num_instances": 1,
       "machine_type_uri": "n1-standard-4",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024}
   },
   "worker_config": {
       "num_instances": 2,
       "machine_type_uri": "n1-standard-4",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024}
   },
}

with models.DAG(
   "dataproc-sa",

   schedule_interval=datetime.timedelta(days=1),
   default_args=default_dag_args) as dag:

   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster",
       project_id=PROJECT_ID,
       cluster_config=CLUSTER_CONFIG,
       region=REGION,
       cluster_name=CLUSTER_NAME
   )

   PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": PYSPARK_URI}
   }

   pyspark_task = DataprocSubmitJobOperator(
       task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
   )

   delete_cluster = DataprocDeleteClusterOperator(
       task_id="delete_cluster",
       project_id=PROJECT_ID,
       cluster_name=CLUSTER_NAME,
       region=REGION
   )

   create_cluster >> pyspark_task >> delete_cluster

    
