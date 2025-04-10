from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests, json, os
import matplotlib.pyplot as plt

def fetch_news():
    headers = {
        "X-Naver-Client-Id": os.getenv("NAVER_CLIENT_ID"),
        "X-Naver-Client-Secret": os.getenv("NAVER_CLIENT_SECRET")
    }
    params = {"query": "AI", "display": 10, "sort": "date"}
    url = "https://openapi.naver.com/v1/search/news.json"
    res = requests.get(url, headers=headers, params=params)
    data = res.json()
    titles = [item["title"] for item in data.get("items", [])]
    with open("/opt/airflow/output/naver_news.json", "w") as f:
        json.dump(titles, f, indent=2)

def visualize():
    with open("/opt/airflow/output/naver_news.json") as f:
        titles = json.load(f)
    lengths = [len(title) for title in titles]
    plt.figure(figsize=(10, 4))
    plt.bar(range(len(lengths)), lengths)
    plt.title("Length of News Titles")
    plt.xlabel("Index")
    plt.ylabel("Length")
    plt.savefig("/opt/airflow/output/naver_chart.png")

with DAG(
    dag_id="naver_news_pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    t1 = PythonOperator(task_id="fetch_news", python_callable=fetch_news)
    t2 = PythonOperator(task_id="visualize", python_callable=visualize)
    t1 >> t2
