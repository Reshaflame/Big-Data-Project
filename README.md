# Data Engineering Final Project

> **🆕 July 2025 Update 2 – Email Analytics Pipeline**
>
> A brand‑new **email pipeline** is live: Iceberg DDL for Bronze → Silver → Gold plus one‑shot CSV loaders. See **What’s new** ⬇️

---

## What’s new

| Area                | Highlights                                                                                                                                                                                                                                                                                                                                           |
| :------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Streaming stack** | <ul><li>📦 `streaming/` compose spins Kafka + Zookeeper + Kafka‑UI.</li><li>🪄 Producers for *sales‑events*, *equipment‑metrics*, *inventory‑updates* **and** *email‑send*.</li></ul>                                                                                                                                                                |
| **Spark jobs**      | <ul><li>🚰 `stream_to_bronze.py` – universal Structured Streaming sink.</li><li>✉️ `ingest_email_stream.py` – dedicated live pipeline for email events.</li><li>🏗️ `init_email_iceberg_tables.py` – creates all Email tables (Bronze/Silver/Gold).</li><li>📥 `load_bronze_email_csvs.py` – bulk‑loads historical email CSVs into Bronze.</li></ul> |
| **Airflow 3 DAGs**  | <ul><li>🆕 `email_stream_ingest` – DockerOperator wrapper for the live email stream.</li><li>🆕 `email_csv_bootstrap` – runs the two new jobs once per env.</li></ul>                                                                                                                                                                                |
| **Makefile**        | <ul><li>`make init-email` – runs the Iceberg DDL + CSV loader end‑to‑end.</li><li>Existing targets (`producers`, `streaming`, `demo`) unchanged.</li></ul>                                                                                                                                                                                           |
| **Compose tweaks**  | Added read‑only mount `../processing/data → /opt/spark-apps/data` so Spark can see bootstrap CSVs.                                                                                                                                                                                                                                                   |

---

## 0 Prerequisites

| Tool                 | Version       | Notes                              |
| -------------------- | ------------- | ---------------------------------- |
| Docker Desktop       | 24 or newer   | Linux: Docker Engine + Compose v2  |
| Docker Compose v2    | bundled       | `docker compose version` to verify |
| Git                  | any recent    | –                                  |
| (Windows only) WSL 2 | Ubuntu 20.04+ | best I/O perf                      |

---

## 1 Clone the project

```bash
# choose any folder you like
git clone https://github.com/<your‑org>/data-eng-project.git
cd data-eng-project
```

---

## 2 Create your personal `.env`

Copy the template and adjust `DATA_ROOT` – everything else works out‑of‑the‑box.

```bash
cp .env.example .env
```

```dotenv
DATA_ROOT=/abs/path/to/data-eng-project
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
AIRFLOW_UID=1000
AIRFLOW_PROJ_DIR=${DATA_ROOT}/orchestration
```

---

## 3 One‑time network

```bash
docker network create data_eng_net   # safe if it exists
```

---

## 4 Spin up the stack

```bash
# Storage + Spark + Iceberg
docker compose -f processing/docker-compose.yml --env-file .env up -d

# Streaming (Kafka)
docker compose -f streaming/docker-compose.yml  --env-file .env up -d

# Orchestration (Airflow 3)
docker compose -f orchestration/docker-compose.yml --env-file .env up -d
```

> **Note:** `processing/docker-compose.yml` now mounts `../processing/data` into the Spark client container at `/opt/spark-apps/data` (read‑only) – required for the email CSV bootstrap.

---

### 4.1 Initialise the Email Iceberg schema and seed Bronze

Run once per environment (or use `make init-email`).

```bash
# create tables
docker exec -it spark-submit spark-submit /opt/jobs/init_email_iceberg_tables.py

# load historical CSVs into Bronze
docker exec -it spark-submit spark-submit /opt/jobs/load_bronze_email_csvs.py
```

You should see ≈100 rows in each `local.bronze.*` table in the Spark console.

---

## 5 Web UIs & credentials

| Component     | URL                                                      | Default creds       | Notes                     |
| ------------- | -------------------------------------------------------- | ------------------- | ------------------------- |
| Airflow 3     | [http://localhost:8080/home](http://localhost:8080/home) | `airflow / airflow` | open `/home` not `/`      |
| MinIO Console | [http://localhost:9001](http://localhost:9001)           | `admin / password`  | change in `.env`          |
| Spark UI      | [http://localhost:4040](http://localhost:4040)           | –                   | only while job is running |
| Iceberg REST  | [http://localhost:8181](http://localhost:8181)           | –                   | programmatic              |
| Kafka UI      | [http://localhost:8090](http://localhost:8090)           | –                   | browse topics             |

---

## 6 Getting Started – Streaming edition 🛰️

```bash
make start        # builds & boots full stack
make init-email   # run the Iceberg DDL + CSV loader
make producers    # start Kafka producers (sales, metrics, inventory, email)
make streaming    # launch Spark Structured Streaming consumer
```

* Monitor Spark → Streaming tab
* Check MinIO → `bronze/` Iceberg files
* Trigger DAG `24sender_batch_etl` in Airflow to push Bronze → Silver → Gold

---

## 7 Stopping / cleaning up

```bash
make stop         # stops all compose stacks
make clean        # removes **all** named volumes – destructive!
```

---

## 8 Troubleshooting

* **404 on Spark UI** – open `<host>:4040` only while a job is active.
* **Airflow connection reset** – always point browser to `/home`.
* **CatalogNotFoundException (`local` vs `spark_catalog`)** – ensure the extra `.config("spark.sql.catalog.local …)` lines are present in every `create_spark_session()`.
* **CSV loader can’t find files** – verify `processing/data/bronze_*` exist and the volume mount is in place.

---

## 9 Reference docs

See [`docs/`](docs/) for architecture diagrams, data models, and a full walkthrough.

Happy hacking & may your streams be ever‑flowing! 🚀
