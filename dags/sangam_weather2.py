import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 전역 변수에 저장된 날씨 응답 텍스트를 XCom으로 넘김
def fetch_weather_response(**context):
    API_KEY = os.getenv("KMA_WEATHER_API_KEY")  # 반드시 URL 인코딩된 키
    BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

    now = datetime.utcnow() + timedelta(hours=9)  # KST
    base_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    base_date = base_time.strftime("%Y%m%d")
    base_time_str = base_time.strftime("%H%M")

    url = (
        f"{BASE_URL}?serviceKey={API_KEY}"
        f"&numOfRows=10&pageNo=1&dataType=XML"
        f"&base_date={base_date}&base_time={base_time_str}&nx=58&ny=126"
    )

    response = requests.get(url)
    response.raise_for_status()

    # 응답 텍스트를 XCom으로 전달
    context['ti'].xcom_push(key='weather_response', value=response.text)

    print(f"✅ API 호출 완료: [{base_date} {base_time_str}]")
    print(response.text)

# 응답 XML 파싱 및 출력
def parse_weather_response(**context):
    response_text = context['ti'].xcom_pull(key='weather_response')
    root = ET.fromstring(response_text)
    items = root.find(".//items")

    print("📡 상암동 날씨 실황:")
    if items is None:
        print("❗ items 태그를 찾을 수 없습니다.")
        print(response_text)
        return

    for item in items.findall("item"):
        category = item.findtext("category")
        value = item.findtext("obsrValue")
        print(f"• {category} → {value}")

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="sangam_weather_pipeline",
    default_args=default_args,
    description="상암동 날씨 정보를 2단계로 처리하는 DAG",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 4, 14),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather_api",
        python_callable=fetch_weather_response,
    )

    parse_task = PythonOperator(
        task_id="parse_weather_data",
        python_callable=parse_weather_response,
    )

    fetch_task >> parse_task