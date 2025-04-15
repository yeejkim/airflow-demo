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
 
 def parse_weather_data(**context):
     response_text = context['ti'].xcom_pull(key='weather_response')
     base_datetime = context['ti'].xcom_pull(key='base_datetime')
 
     root = ET.fromstring(response_text)
     items = root.find(".//items")
 
     if items is None:
         print("❗ items 태그 없음")
         print(response_text)
         return
 
     parsed_lines = [f"\ud83d\udcf1 *{base_datetime} 기준 상암동 날씨 실황*"]
 
     for item in items.findall("item"):
         category = item.findtext("category")
         value = item.findtext("obsrValue")
 
         if category == "PTY":
             desc = PTY_CODE_MAP.get(value, "알 수 없음")
             parsed_lines.append(f"\u2614\ufe0f 강수 형태: {desc} (코드: {value})")
         elif category == "REH":
             parsed_lines.append(f"\ud83d\udca7 습도: {value}%")
         elif category == "T1H":
             parsed_lines.append(f"\ud83c\udf21\ufe0f 기온: {value}\u2103")
         elif category == "WSD":
             parsed_lines.append(f"\ud83c\udf2c\ufe0f 풍속: {value} m/s")
 
     context['ti'].xcom_push(key='slack_message', value="\n".join(parsed_lines))
 
 def send_slack_notification(**context):
     SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
     message = context['ti'].xcom_pull(key='slack_message')
 
     slack_payload = {"text": message}
     slack_response = requests.post(SLACK_WEBHOOK_URL, json=slack_payload)
     if slack_response.status_code != 200:
         print("슬랙 알림 실패:", slack_response.text)
 
 
 default_args = {
     "owner": "airflow",
     "retries": 1,
     "retry_delay": timedelta(minutes=5),
 }
 
 with DAG(
     dag_id="sangam_weather_slack",
     default_args=default_args,
     description="상암동 날씨를 Slack으로 알림",
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
 
     notify_task = PythonOperator(
         task_id="send_slack_notification",
         python_callable=send_slack_notification,
     )
 
     fetch_task >> parse_task >> notify_task