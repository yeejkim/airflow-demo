import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_base_datetime():
    now = datetime.utcnow() + timedelta(hours=9)  # 한국 시간
    base_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return base_time.strftime("%Y%m%d"), base_time.strftime("%H%M")

def fetch_kma_weather():
    base_date, base_time = get_base_datetime()
    API_KEY = os.getenv("KMA_WEATHER_API_KEY")

    params = {
        "serviceKey": API_KEY,
        "numOfRows": "10",
        "pageNo": "1",
        "dataType": "XML",
        "base_date": base_date,
        "base_time": base_time,
        "nx": "58",   # 마포구 상암동 격자
        "ny": "126"
    }

    response = requests.get(
        "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst",
        params=params
    )
    response.raise_for_status()

    root = ET.fromstring(response.content)
    items = root.find(".//items")

    print(f"📡 [{base_date} {base_time}] 서울 마포구 상암동 날씨 실황:")

    if items is None:
        print("❗ items 태그를 찾을 수 없습니다. 응답 확인 필요.")
        print(response.text)
        return

    for item in items.findall("item"):
        category = item.findtext("category")
        obsrValue = item.findtext("obsrValue")
        print(f"• {category} → {obsrValue}")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kma_weather_sangam",
    default_args=default_args,
    description="상암동 날씨 실황을 1시간마다 출력하는 DAG",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 4, 14),
    catchup=False,
) as dag:

    fetch_weather_task = PythonOperator(
        task_id="fetch_and_print_weather_sangam",
        python_callable=fetch_kma_weather,
    )