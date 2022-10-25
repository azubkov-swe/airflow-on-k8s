from os import path
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3
}

dag = DAG(
    'ezaf_spark_parquet',
    default_args=default_args,
    schedule_interval=None,
    tags=['example', 'ezaf', 'spark', 'parquet'],
    params={
        'namespace': "spark",
    }
)

submit = SparkKubernetesOperator(
    task_id='ezaf_spark_parquet_submit',
    namespace="spark",
    application_file="example_ezaf_spark_parquet.yaml",
    kubernetes_conn_id="kubernetes_in_cluster",
    do_xcom_push=True,
    dag=dag,
    api_group="sparkoperator.hpe.com",
    enable_impersonation_from_ldap_user=False
)

sensor = SparkKubernetesSensor(
    task_id='ezaf_spark_parquet_monitor',
    namespace="spark",
    application_name="{{ task_instance.xcom_pull(task_ids='ezaf_spark_parquet_submit')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_in_cluster",
    dag=dag,
    api_group="sparkoperator.hpe.com",
    attach_log=True
)

submit >> sensor
