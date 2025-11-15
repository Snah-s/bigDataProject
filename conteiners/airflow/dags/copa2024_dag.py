from datetime import datetime, timedelta
from pathlib import Path
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

# config
MONGO_URI = "mongodb://root:example@mongo:27017"
DB_NAME = "copa2024"
KPIS_COLLECTION = "kpis"


def run_loader_script(**context):
  dag_dir = Path(__file__).resolve().parent
  script_path = dag_dir / "load_copa32_to_mongo.py"

  cmd = ["python", str(script_path)]
  subprocess.run(cmd, check=True, text=True)


def compute_team_kpis(**context):
  client = MongoClient(MONGO_URI, authSource="admin")
  db = client[DB_NAME]

  events = db["events"]
  col_kpis = db[KPIS_COLLECTION]

  pipeline = [
    {"$match": {"type": "Shot"}},
    {"$group": {
      "_id": {
        "team_id": "$team_id",
        "team": "$team"
      },
      "total_shots": {"$sum": 1},
      "matches": {"$addToSet": "$match_id"}
    }},
    {"$project": {
      "_id": 0,
      "team_id": "$_id.team_id",
      "team": "$_id.team",
      "total_shots": 1,
      "matches_count": {"$size": "$matches"},
      "shots_per_match": {
        "$cond": [
          {"$gt": [{"$size": "$matches"}, 0]},
          {"$divide": ["$total_shots", {"$size": "$matches"}]},
          0
        ]
      }
    }}
  ]

  results = list(events.aggregate(pipeline))

  col_kpis.delete_many({"level": "team"})

  if results:
    docs = []
    for r in results:
      docs.append({
        "level": "team",
        "team_id": r["team_id"],
        "team": r["team"],
        "metrics": {
          "total_shots": r["total_shots"],
          "matches": r["matches_count"],
          "shots_per_match": r["shots_per_match"],
        },
        "updated_at": datetime.utcnow()
      })
    col_kpis.insert_many(docs)


default_args = {
  "owner": "copa2024",
  "depends_on_past": False,
  "retries": 1,
  "retry_delay": timedelta(minutes=5),
}

with DAG(
  dag_id="copa2024_pipeline",
  default_args=default_args,
  description="ETL + KPIs Copa América 2024 hacia MongoDB",
  schedule_interval=None,
  start_date=datetime(2025, 1, 1),
  catchup=False,
  tags=["copa2024", "mongo", "bigdata"],
) as dag:

  load_events = PythonOperator(
    task_id="load_events_to_mongo",
    python_callable=run_loader_script,
  )

  compute_kpis_task = PythonOperator(
    task_id="compute_team_kpis",
    python_callable=compute_team_kpis,
  )

  load_events >> compute_kpis_task
