# Data Engineering Final Project

This repository contains a **local‑first, fully containerised** data‑engineering stack built around **Spark + Iceberg + MinIO + Kafka + Airflow 3**.  Follow the steps below and you’ll have the whole system running on any Windows/macOS/Linux laptop in under five minutes.

---

## 0  Prerequisites

| Tool                 | Version                     | Notes                                          |
| -------------------- | --------------------------- | ---------------------------------------------- |
| Docker Desktop       |  24 or newer                | Linux users can use Docker Engine + Compose v2 |
| Docker Compose v2    | bundled with Docker Desktop | run `docker compose version` to verify         |
| Git                  | any recent                  | ‑                                              |
| (Windows only) WSL 2 | Ubuntu 20.04+               | recommended for best I/O performance           |

---

## 1  Create / clone the project

```bash
# choose any folder you like
git clone https://github.com/<your‑org>/data-eng-project.git
cd data-eng-project
```

---

## 2  Create your personal **.env** file

The project ships with ``.  Copy it and adjust the absolute path to where you cloned the repo.

```bash
cp .env.example .env
# then edit .env
```

```dotenv
# .env ------------------------------------------------------
# root of the repo on *your* machine
DATA_ROOT=/absolute/path/to/data-eng-project

# object‑storage credentials (feel free to change)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password

# optional: Airflow tweaks
AIRFLOW_UID=1000               # UID inside containers
AIRFLOW_PROJ_DIR=${DATA_ROOT}/orchestration  # don’t touch
```

> **Everyone sets their own path**.  The file is *git‑ignored*, so it never clutters the repo.

---

## 3  One‑time network

```bash
docker network create data_eng_net   # safe to re‑run if it already exists
```

---

## 4  Spin up the stack

```bash
# Storage + processing (MinIO, Spark, Iceberg REST)
cd processing && docker compose --env-file ../.env up -d && cd ..

# Streaming (Kafka)
cd streaming && docker compose --env-file ../.env up -d && cd ..

# Orchestration (Airflow 3 + Postgres + Redis)
cd orchestration && docker compose --env-file ../.env up -d && cd ..
```

### First‑run timings (cold start)

| Stack         | Time    |
| ------------- | ------- |
| Processing    | ≈ 1 min |
| Streaming     | 30 s    |
| Orchestration | 2 min   |

---

## 5  Web UIs & credentials

| Component         | URL                                                        | Default creds       | Notes                                                      |
| ----------------- | ---------------------------------------------------------- | ------------------- | ---------------------------------------------------------- |
| **Airflow 3 UI**  | [`http://localhost:8080/home`](http://localhost:8080/home) | `airflow / airflow` | The root path `/` returns *connection reset*; use `/home`. |
| **MinIO Console** | [`http://localhost:9001`](http://localhost:9001)           | `admin / password`  | Change in `.env` if you like.                              |
| **Spark UI**      | [`http://localhost:4040`](http://localhost:4040)           | –                   | Visible only while a Spark job is running.                 |
| **Iceberg REST**  | [`http://localhost:8181`](http://localhost:8181)           | –                   | Programmatic access only.                                  |

---

## 6  Stopping / cleaning up

```bash
# stop containers (keep volumes)
cd processing      && docker compose down && cd ..
cd streaming       && docker compose down && cd ..
cd orchestration   && docker compose down && cd ..

# free disk (delete all named volumes)
docker volume rm $(docker volume ls -q)
```

---

## 7  (Advanced) Local override

If you want Postgres **outside** Docker’s internal volume store, create an **uncharted** file `orchestration/docker-compose.override.yml`:

```yaml
services:
  postgres:
    volumes:
      - /ext4/path/airflow-pgdata:/var/lib/postgresql/data
```

The override is `.gitignore`d, so it only affects your machine.

---

## 8  Troubleshooting

- `connection reset` on port 8080 → remember to visit `/home`, not `/`.
- **Permission errors** in Airflow logs on Windows → run `chmod -R 777 orchestration/{logs,dags,plugins}` inside WSL.
- **Port already in use** → edit the `ports:` section and pick a free host port.

Happy hacking!

