## 🔟 Appendix – Manual Pipeline Execution (⏱ Interim Path)

> **Heads‑up 🚧** Until the Airflow DAGs are finished, you can still run the full pipeline end‑to‑end with the CLI recipe below. These commands replicate what the DAGs will do automatically, so our lecturer/reviewer can reproduce the demo.

---

### 1 Prepare Docker networks

```bash
docker network create data_eng_net          # safe if already exists
docker network create 24sender-network     # stack‑internal
```

### 2 Spin‑up the three core stacks

```bash
# Storage + Spark + Iceberg
docker compose -f processing/docker-compose.yml      --env-file .env up -d

# Streaming (Kafka)
docker compose -f streaming/docker-compose.yml       --env-file .env up -d

# Orchestration shell (Airflow 3 – optional for now)
docker compose -f orchestration/docker-compose.yml    --env-file .env up -d
```

Verify everything with:

```bash
docker ps
```

---

### 3 Iceberg bootstrap (schema + historical seed)

```bash
# give Spark full RW access to the local warehouse first
docker exec -u 0 -it spark-submit mkdir -p /home/iceberg/warehouse
docker exec -u 0 -it spark-submit chmod -R 777 /home/iceberg

# 3.1 Create all e‑mail tables (Bronze → Silver → Gold)
docker exec -it spark-submit spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 \
  /opt/jobs/init_iceberg_email_pipeline.py

# 3.2 Upload historical CSV snapshots into Bronze
#    (the live Kafka stream will pick up from here)
docker exec -it spark-submit mkdir -p /tmp/data/bronze
docker cp $PWD/processing/jobs/data/bronze/. spark-submit:/tmp/data/bronze
```

**Quick sanity‑check**

```bash
# List Bronze tables
docker exec -it spark-submit spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/home/iceberg/warehouse \
  -e "SHOW TABLES IN local.bronze;"
```

Open MinIO → [http://localhost:9001](http://localhost:9001) and make sure objects appear under `bronze/`.

---

### 4 Manual ETL (Bronze → Silver → Gold)

> *Skip this section if you only need Bronze tables.*

```bash
# Bronze → Silver
docker cp processing/jobs/bronze_to_silver.py spark-submit:/opt/bronze_to_silver.py

docker exec -it spark-submit spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/home/iceberg/warehouse \
  /opt/bronze_to_silver.py

# Silver → Gold (optional)
docker cp processing/jobs/silver_to_gold.py spark-submit:/opt/silver_to_gold.py

docker exec -it spark-submit spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/home/iceberg/warehouse \
  /opt/silver_to_gold.py
```

*Check Silver tables*

```bash
docker exec -it spark-submit spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/home/iceberg/warehouse \
  -e "SHOW TABLES IN local.silver;"
```

---

### 5 Clean‑up

```bash
make stop   # or run docker compose down per stack
make clean  # ⚠️ DESTROYS all named volumes
```

---

When the fully‑automated DAGs land, this appendix will be removed. Until then: happy streaming & enjoy the hands‑on tour! ✨

