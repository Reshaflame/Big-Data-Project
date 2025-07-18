from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="email_stream_ingest",
    start_date=days_ago(0),
    schedule_interval=None,          # trigger manually; it's a *long-running* stream
    catchup=False,
    tags=["streaming"],
) as dag:

    spark_ingest = DockerOperator(
        task_id="spark_stream",
        image="tabulario/spark-iceberg:latest",
        api_version="auto",
        auto_remove=True,
        command="/opt/jobs/spark_submit.sh",
        docker_url="unix://var/run/docker.sock",
        network_mode="data_eng_net",
        mounts=[
            "/$DATA_ROOT/processing/warehouse:/home/iceberg/warehouse",
            "/$DATA_ROOT/processing/jobs:/opt/jobs",
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "{{ var.value.MINIO_ROOT_USER }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.MINIO_ROOT_PASSWORD }}",
        },
    )
