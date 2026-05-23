from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

# Pakistani cities with coordinates
CITIES = [
    {"name": "Karachi",     "lat": 24.8607, "lon": 67.0011},
    {"name": "Lahore",      "lat": 31.5497, "lon": 74.3436},
    {"name": "Islamabad",   "lat": 33.6844, "lon": 73.0479},
    {"name": "Peshawar",    "lat": 34.0151, "lon": 71.5249},
    {"name": "Quetta",      "lat": 30.1798, "lon": 66.9750},
]

default_args = {
    'owner': 'hamza',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def fetch_weather_data(**context):
    """Extract: Pull weather data from Open-Meteo API"""
    all_data = []
    
    for city in CITIES:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={city['lat']}&longitude={city['lon']}"
            f"&current=temperature_2m,relative_humidity_2m,"
            f"wind_speed_10m,weather_code,precipitation"
            f"&timezone=Asia/Karachi"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        current = data['current']
        all_data.append({
            'city':        city['name'],
            'timestamp':   current['time'],
            'temperature': current['temperature_2m'],
            'humidity':    current['relative_humidity_2m'],
            'wind_speed':  current['wind_speed_10m'],
            'precipitation': current['precipitation'],
            'weather_code': current['weather_code'],
            'fetched_at':  datetime.now().isoformat()
        })
        print(f"Fetched weather for {city['name']}: {current['temperature_2m']}°C")
    
    # Push to XCom so next task can access it
    context['ti'].xcom_push(key='weather_data', value=all_data)
    print(f"Successfully fetched data for {len(all_data)} cities")

def transform_weather_data(**context):
    """Transform: Clean and enrich the data"""
    raw_data = context['ti'].xcom_pull(key='weather_data', task_ids='fetch_weather')
    
    df = pd.DataFrame(raw_data)
    
    def get_condition(code):
        if code == 0:                return 'Clear sky'
        elif code in [1, 2, 3]:     return 'Cloudy'
        elif code in range(51, 68): return 'Drizzle/Rain'
        elif code in range(71, 78): return 'Snow'
        elif code in range(80, 83): return 'Rain showers'
        elif code in range(95, 100): return 'Thunderstorm'
        else:                        return 'Unknown'
    
    df['condition']         = df['weather_code'].apply(get_condition)
    df['temp_fahrenheit']   = (df['temperature'] * 9/5) + 32
    
    # Extract date and hour BEFORE converting to string
    dt_series               = pd.to_datetime(df['timestamp'])
    df['date']              = dt_series.dt.strftime('%Y-%m-%d')
    df['hour']              = dt_series.dt.hour
    
    # Convert timestamp to string LAST so XCom can serialize it
    df['timestamp']         = dt_series.dt.strftime('%Y-%m-%d %H:%M:%S')
    
    transformed = df.to_dict(orient='records')
    context['ti'].xcom_push(key='transformed_data', value=transformed)
    print(f"Transformed {len(transformed)} records")
    
def load_to_postgres(**context):
    """Load: Insert data into PostgreSQL"""
    data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_weather')
    
    hook = PostgresHook(postgres_conn_id='postgres_weather')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO weather_data (
            city, timestamp, temperature, humidity,
            wind_speed, precipitation, weather_code,
            condition, temp_fahrenheit, date, hour, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for record in data:
        cursor.execute(insert_query, (
            record['city'],
            record['timestamp'],
            record['temperature'],
            record['humidity'],
            record['wind_speed'],
            record['precipitation'],
            record['weather_code'],
            record['condition'],
            record['temp_fahrenheit'],
            record['date'],
            record['hour'],
            record['fetched_at']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(data)} records into PostgreSQL")

# Define the DAG
with DAG(
    dag_id='weather_pipeline_pakistan',
    default_args=default_args,
    description='Daily weather ETL for Pakistani cities',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'pakistan', 'etl'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_weather',
        sql="""
            CREATE TABLE IF NOT EXISTS weather_data (
                id              SERIAL PRIMARY KEY,
                city            VARCHAR(50),
                timestamp       TIMESTAMP,
                temperature     FLOAT,
                humidity        INTEGER,
                wind_speed      FLOAT,
                precipitation   FLOAT,
                weather_code    INTEGER,
                condition       VARCHAR(50),
                temp_fahrenheit FLOAT,
                date            VARCHAR(20),
                hour            INTEGER,
                fetched_at      VARCHAR(50)
            );
        """,
    )

    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
    )

    transform_weather = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather_data,
    )

    load_weather = PythonOperator(
        task_id='load_weather',
        python_callable=load_to_postgres,
    )

    # Task dependencies — this is the DAG structure
    create_table >> fetch_weather >> transform_weather >> load_weather