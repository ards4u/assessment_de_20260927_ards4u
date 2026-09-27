# Daily Weather Pipeline

Open-Meteo (free, no key) → Postgres → dbt (staging → mart) → Airflow, plus
a notebook that proves it all runs end-to-end.

```
weather_pipeline/
├── extract_load/           # API client, Postgres upsert, run()/backfill()
├── sql/create_raw_table.sql
├── dbt/weather_pipeline/    # sources, staging, marts, schema tests
├── airflow/dags/            # extract_and_load >> dbt_run >> dbt_test
├── notebooks/walkthrough.ipynb
├── docker-compose.yml       # local Postgres only
└── .env.example
```

## Setup

```bash
cp .env.example .env && export $(cat .env | xargs)
docker compose up -d                      # local Postgres on :5432
pip install -r requirements.txt
cp dbt/weather_pipeline/profiles.yml.example ~/.dbt/profiles.yml
```

## Run it manually (no Airflow)

```bash
python -m extract_load.pipeline --logical-date 2024-06-01
# or a range:
python -m extract_load.pipeline --backfill 2024-05-01 2024-06-01

cd dbt/weather_pipeline
dbt run
dbt test
```

## Run it with Airflow

Mount/symlink this repo to `/opt/airflow/dags/repo` (or edit `REPO_ROOT` in
`airflow/dags/weather_pipeline_dag.py`), put `dags/` on `AIRFLOW__CORE__DAGS_FOLDER`,
and unpause `daily_weather_pipeline`. Use `airflow dags backfill` for history.

## Study it in the notebook

```bash
jupyter notebook notebooks/walkthrough.ipynb
```

Runs extract/load, reruns it on the same date to prove idempotency, runs
`dbt run` + `dbt test`, then queries `mart_daily_weather` with pandas.
