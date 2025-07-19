from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG("test_docker", start_date=days_ago(0), schedule_interval=None, catchup=False) as dag:
    test = DockerOperator(
        task_id="hello_container",
        image="busybox",
        command="echo Hello from DockerOperator",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

with DAG(
    dag_id="email_stream_ingest",
    start_date=days_ago(0),
    schedule_interval=None,
    catchup=False,
    tags=["streaming"],
) as dag:

    spark_ingest = DockerOperator(
        task_id="spark_stream",
        image="tabulario/spark-iceberg:latest",
        api_version="auto",
        auto_remove=True,
        command="/bin/bash -c '/opt/jobs/spark_submit.sh'",
        docker_url="unix://var/run/docker.sock",
        network_mode="data_eng_net",

        # Use Mount objects instead of raw strings
        mounts=[
            Mount(source="/mnt/h/data-eng-project/processing/warehouse", target="/home/iceberg/warehouse", type="bind"),
            Mount(source="/mnt/h/data-eng-project/processing/jobs", target="/opt/jobs", type="bind"),
            Mount(source="/mnt/h/data-eng-project/processing/libs", target="/opt/spark/external-jars", type="bind"),
        ],

        environment={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
        },

        tty=True,
        mount_tmp_dir=False,
    )
