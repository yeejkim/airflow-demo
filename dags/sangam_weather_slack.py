import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# PTY 코드 해석용
PTY_CODE_MAP = {
    "0": "없음",
    "1": "비",
    "2": "비/눈",
    "3": "눈",
    "4": "소나기",
}

def get_base_datetime():
    now = datetime.utcnow() + timedelta(hours=9)
    base_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return base_time.strftime("%Y%m%d"), base_time.strftime("%H%M")

def fetch_weather_data(**context):
    base_date, base_time = get_base_datetime()
    API_KEY = os.getenv("KMA_WEATHER_API_KEY")

    params = {
        "serviceKey": API_KEY,
        "numOfRows": "10",
        "pageNo": "1",
        "dataType": "XML",
        "base_date": base_date,
        "base_time": base_time,
        "nx": "58",
        "ny": "126"
    }

    response = requests.get("http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst", params=params)
    response.raise_for_status()

    context['ti'].xcom_push(key='weather_response', value=response.text)
    context['ti'].xcom_push(key='base_datetime', value=f"{base_date} {base_time}")
    print("✅ fetch_weather_data 완료")

def parse_weather_data(**context):
    response_text = context['ti'].xcom_pull(key='weather_response')
    base_datetime = context['ti'].xcom_pull(key='base_datetime')

    root = ET.fromstring(response_text)
    items = root.find(".//items")

    if items is None:
        print("❗ items 태그 없음")
        print(response_text)
        return

    weather_data = {
        "base_datetime": base_datetime,
        "PTY": None,
        "REH": None,
        "T1H": None,
        "WSD": None
    }

    for item in items.findall("item"):
        category = item.findtext("category")
        value = item.findtext("obsrValue")
        if category in weather_data:
            weather_data[category] = value

    context['ti'].xcom_push(key='parsed_weather', value=weather_data)
    print("✅ parse_weather_data 완료:", weather_data)

def save_to_txt_file(**context):
    data = context['ti'].xcom_pull(key='parsed_weather')
    output_dir = "/opt/airflow/output"
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{output_dir}/weather_{data['base_datetime'].replace(' ', '_')}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"📱 *{data['base_datetime']} 기준 상암동 날씨 실황*\n")
        f.write(f"☔️ 강수 형태: {PTY_CODE_MAP.get(data['PTY'], '알 수 없음')} (코드: {data['PTY']})\n")
        f.write(f"💧 습도: {data['REH']}%\n")
        f.write(f"🌡️ 기온: {data['T1H']}℃\n")
        f.write(f"🌬️ 풍속: {data['WSD']} m/s\n")

    print(f"✅ 파일 저장 완료: {filename}")

def send_slack_notification(**context):
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
    data = context['ti'].xcom_pull(key='parsed_weather')

    message = (
        f"📱 *{data['base_datetime']} 기준 상암동 날씨 실황*\n"
        f"☔️ 강수 형태: {PTY_CODE_MAP.get(data['PTY'], '알 수 없음')} (코드: {data['PTY']})\n"
        f"💧 습도: {data['REH']}%\n"
        f"🌡️ 기온: {data['T1H']}℃\n"
        f"🌬️ 풍속: {data['WSD']} m/s"
    )

    slack_payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK_URL, json=slack_payload)

    if response.status_code != 200:
        print("❌ 슬랙 알림 실패:", response.text)
    else:
        print("✅ 슬랙 알림 전송 완료")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sangam_weather_slack",
    default_args=default_args,
    description="상암동 날씨 데이터 저장 및 Slack 알림",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 4, 14),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    parse_task = PythonOperator(
        task_id="parse_weather_data",
        python_callable=parse_weather_data,
    )

    save_task = PythonOperator(
        task_id="save_to_txt_file",
        python_callable=save_to_txt_file,
    )

    notify_task = PythonOperator(
        task_id="send_slack_notification",
        python_callable=send_slack_notification,
    )

    fetch_task >> parse_task
    parse_task >> [save_task, notify_task]