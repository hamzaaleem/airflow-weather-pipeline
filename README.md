# 🌤️ Pakistan Weather ETL Pipeline — Apache Airflow


<img width="1536" height="1024" alt="architechture diagram" src="https://github.com/user-attachments/assets/9d711d47-a794-407d-96a8-2f1dc6b711d8" />

An automated batch ETL pipeline that pulls real-time weather data for 5 major Pakistani cities daily, transforms it, loads it into PostgreSQL, and visualizes it in Power BI — fully orchestrated with Apache Airflow running in Docker.

---

## 📐 Architecture

```
Open-Meteo API (free, no signup)
        │
        │  HTTP requests
        ▼
┌─────────────────────────────────┐
│     Apache Airflow DAG          │  ← runs every day automatically
│                                 │
│  create_table                   │
│       ↓                         │
│  fetch_weather   (Extract)      │
│       ↓  XCom                   │
│  transform_weather (Transform)  │
│       ↓  XCom                   │
│  load_weather    (Load)         │
└─────────────────────────────────┘
        │
        │  PostgreSQL Hook
        ▼
PostgreSQL (Docker)
        │
        │  ODBC
        ▼
Power BI Dashboard
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Data source | Open-Meteo API (free, no key required) |
| Transformation | Python, Pandas |
| Storage | PostgreSQL 15 (Docker) |
| Containerization | Docker + Docker Compose |
| Visualization | Power BI Desktop |

---

## 📁 Project Structure

```
airflow-weather-pipeline/
├── docker-compose.yml          # Airflow + PostgreSQL services
├── dags/
│   └── weather_pipeline.py     # Main DAG — ETL logic
├── logs/                       # Airflow task logs (auto-generated)
├── plugins/                    # Custom plugins (empty for now)
└── README.md
```

---

## 🌍 Cities Tracked

| City | Latitude | Longitude |
|---|---|---|
| Karachi | 24.8607 | 67.0011 |
| Lahore | 31.5497 | 74.3436 |
| Islamabad | 33.6844 | 73.0479 |
| Peshawar | 34.0151 | 71.5249 |
| Quetta | 30.1798 | 66.9750 |

---

## 📊 Data Collected

| Field | Description |
|---|---|
| `temperature` | Current temperature in °C |
| `temp_fahrenheit` | Temperature converted to °F |
| `humidity` | Relative humidity % |
| `wind_speed` | Wind speed in km/h |
| `precipitation` | Precipitation in mm |
| `condition` | Human-readable weather condition |
| `weather_code` | WMO weather code |
| `date` | Date of reading |
| `hour` | Hour of reading |

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Power BI Desktop

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/airflow-weather-pipeline.git
cd airflow-weather-pipeline
```

### 2. Start all services
```bash
docker compose up -d
```

### 3. Create Airflow admin user
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

### 4. Open Airflow UI
```
http://localhost:8080
```
Login: `admin` / `admin`

### 5. Add PostgreSQL connection
- Go to **Admin → Connections → +**
- Connection Id: `postgres_weather`
- Connection Type: `Postgres`
- Host: `postgres_weather_db`
- Database: `weather_db`
- Login: `weather`
- Password: `weather`
- Port: `5432`

### 6. Enable and trigger the DAG
- Find `weather_pipeline_pakistan` in the DAGs list
- Toggle it on
- Click ▶ to trigger manually

### 7. Verify data in PostgreSQL
```bash
docker exec -it postgres_weather_db psql -U weather -d weather_db
```
```sql
SELECT city, temperature, condition, humidity, date
FROM weather_data
ORDER BY city;
```

---

## 🔄 DAG Structure

```
create_table → fetch_weather → transform_weather → load_weather
```

| Task | Type | Description |
|---|---|---|
| `create_table` | PostgresOperator | Creates table if not exists |
| `fetch_weather` | PythonOperator | Calls Open-Meteo API for all 5 cities |
| `transform_weather` | PythonOperator | Adds condition label, fahrenheit, date, hour |
| `load_weather` | PythonOperator | Inserts records into PostgreSQL |

Data is passed between tasks using **XCom** — Airflow's built-in inter-task communication.

---

## 📊 Power BI Dashboard

Connect Power BI via ODBC:
- Driver: PostgreSQL Unicode
- Server: `localhost`
- Port: `5433`
- Database: `weather_db`

Dashboard includes:
- **Bar chart** — latest temperature per city
- **Line chart** — temperature trend over time
- **Pie chart** — weather conditions distribution
- **Scatter chart** — humidity vs temperature
- **KPI cards** — max/min temperature, total records

---

## ⚠️ Known Issues & Fixes

**Timestamp not JSON serializable** — pandas Timestamps must be converted to strings before XCom push. Use `.dt.strftime('%Y-%m-%d %H:%M:%S')`.

**Airflow webserver high CPU** — reduced to single container with `--workers 1` and scheduler combined. Set `mem_limit: 2g` in compose file.

**Power BI shows summarized values** — duplicate the table in Power Query, apply `Remove Duplicates` on city column for latest snapshot view, keep original for historical trends.

---

## 📚 Key Concepts Learned

- **DAG** — Directed Acyclic Graph, a pipeline of dependent tasks
- **XCom** — how Airflow passes data between tasks
- **Operators** — PythonOperator, PostgresOperator
- **Hooks** — PostgresHook for database connections
- **Schedule interval** — `@daily` runs automatically every 24 hours
- **Retries** — automatic retry on failure with configurable delay

---

## 🗺️ Project Roadmap

- [x] Level 0 — Smart City real-time streaming pipeline
- [x] Level 1 — Batch ETL with Airflow (this project)
- [ ] Level 2 — AWS Glue ETL + Redshift data warehouse
- [ ] Level 3 — dbt + Snowflake + Airflow
- [ ] Level 4 — Pakistan e-commerce capstone

---

## 🙏 Credits

Weather data provided by [Open-Meteo](https://open-meteo.com/) — free, open-source weather API.

