# Weather ETL Pipeline — Pakistan Cities

An automated batch data pipeline that pulls real-time weather data for 5 major Pakistani cities daily, transforms it, loads it into PostgreSQL, and visualizes it in Power BI — all orchestrated with Apache Airflow running in Docker.

---

## Architecture

```
Airflow Scheduler (daily trigger)
        │
        ▼
┌─────────────────────────────────────────┐
│         DAG: weather_pipeline_pakistan  │
│                                         │
│  create_table → fetch_weather           │
│                      │                  │
│               transform_weather         │
│                      │                  │
│                load_weather             │
└─────────────────────────────────────────┘
        │                    │
        ▼                    ▼
  PostgreSQL          Open-Meteo API
  weather_db          (free, no auth)
        │
        ▼
  Power BI Dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Data source | Open-Meteo API (free, no API key) |
| Transformation | Python, Pandas |
| Storage | PostgreSQL 15 |
| Infrastructure | Docker + Docker Compose |
| Visualization | Power BI Desktop |

---

## Cities Covered

| City | Latitude | Longitude |
|---|---|---|
| Karachi | 24.8607 | 67.0011 |
| Lahore | 31.5497 | 74.3436 |
| Islamabad | 33.6844 | 73.0479 |
| Peshawar | 34.0151 | 71.5249 |
| Quetta | 30.1798 | 66.9750 |

---

## Data Collected

| Field | Description |
|---|---|
| `temperature` | Current temperature (°C) |
| `temp_fahrenheit` | Temperature converted to °F |
| `humidity` | Relative humidity (%) |
| `wind_speed` | Wind speed (km/h) |
| `precipitation` | Precipitation (mm) |
| `condition` | Weather condition (Clear/Cloudy/Rain/Snow/Thunderstorm) |
| `date` | Date of reading |
| `hour` | Hour of reading |

---

## Project Structure

```
airflow-weather-pipeline/
├── docker-compose.yml          # Airflow + PostgreSQL containers
├── dags/
│   └── weather_pipeline.py     # Main DAG with all 4 tasks
├── logs/                       # Airflow task logs (auto-generated)
└── plugins/                    # Empty (reserved for custom plugins)
```

---

## Getting Started

### Prerequisites
- Docker Desktop
- Power BI Desktop

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/airflow-weather-pipeline.git
cd airflow-weather-pipeline
```

### 2. Create required folders
```bash
mkdir logs plugins
```

### 3. Start containers
```bash
docker compose up -d
```

Wait 2-3 minutes for Airflow to initialize.

### 4. Create Airflow user (if not auto-created)
```bash
docker exec -it airflow bash
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 5. Open Airflow UI
Go to `http://localhost:8080` and login with `admin` / `admin`

### 6. Add PostgreSQL connection
Admin → Connections → Add:

| Field | Value |
|---|---|
| Connection Id | `postgres_weather` |
| Connection Type | `Postgres` |
| Host | `postgres_weather_db` |
| Database | `weather_db` |
| Login | `weather` |
| Password | `weather` |
| Port | `5432` |

### 7. Trigger the DAG
DAGs → `weather_pipeline_pakistan` → Enable → Trigger

### 8. Connect Power BI
- Install [npgsql driver](https://github.com/npgsql/npgsql/releases/download/v4.0.14/Npgsql-4.0.14.msi)
- Get Data → PostgreSQL → `localhost:5433` / `weather_db`
- Username: `weather` / Password: `weather`

---

## DAG Tasks

| Task | Operator | What it does |
|---|---|---|
| `create_table` | PostgresOperator | Creates `weather_data` table if not exists |
| `fetch_weather` | PythonOperator | Calls Open-Meteo API for all 5 cities |
| `transform_weather` | PythonOperator | Adds condition label, Fahrenheit, date, hour |
| `load_weather` | PythonOperator | Inserts transformed records into PostgreSQL |

---

## Power BI Dashboard

- Line chart — temperature trend per city over time
- Bar chart — average humidity per city
- Pie chart — weather conditions distribution
- Scatter chart — humidity vs temperature
- Cards — max/min temperature, total records

---

## Key Concepts Learned

- **DAG** — Directed Acyclic Graph, the pipeline definition in Airflow
- **XCom** — how Airflow tasks pass data between each other
- **Operator** — a single unit of work (PythonOperator, PostgresOperator)
- **Connection** — how Airflow stores credentials for external systems
- **Schedule interval** — `@daily` triggers the pipeline once per day automatically

---

## Lessons Learned

- Pandas `Timestamp` objects are not JSON serializable — extract date/hour before converting to string
- Airflow webserver and scheduler combined in one container reduces resource usage significantly
- XCom is best for small data (< 1MB) — for large datasets write to a file or database between tasks

---

## What's Next

This project is part of a 4-level data engineering portfolio:

- ✅ Level 0 — Smart City real-time streaming (Kafka + Spark + AWS)
- ✅ Level 1 — Batch ETL with Airflow (this project)
- 🔜 Level 2 — AWS Glue ETL + Redshift
- 🔜 Level 3 — dbt + Snowflake + Airflow
- 🔜 Level 4 — Pakistan e-commerce capstone
