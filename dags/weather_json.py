import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# 환경 변수에서 API 키 가져오기
API_KEY = os.getenv("OPEN_WEATHER_API_KEY")  # .env에 저장되어 있어야 함
CITY = "Seoul"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
OUTPUT_DIR = "/opt/airflow/data/weather_json/"

# 날씨 정보 수집 함수
def fetch_weather_and_save_json():
    if not API_KEY:
        raise ValueError("API Key is missing!")

    # API 요청
    params = {"q": CITY, "appid": API_KEY, "units": "metric"}
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    # 필요한 데이터 정리
    weather_data = {
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["description"],
        "timestamp": datetime.utcnow().isoformat()
    }

    # 저장 디렉토리 없으면 생성
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 파일 이름 생성
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    file_path = os.path.join(OUTPUT_DIR, f"weather_{timestamp}.json")

    # JSON 저장
    with open(file_path, "w") as f:
        json.dump(weather_data, f, indent=2)

# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_to_json",
    default_args=default_args,
    description="Fetch weather data and save as JSON",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    fetch_and_save_json_task = PythonOperator(
        task_id="fetch_weather_json",
        python_callable=fetch_weather_and_save_json,
    )