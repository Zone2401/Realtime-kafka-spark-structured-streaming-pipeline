#!/usr/bin/env bash
set -euo pipefail

if [ -f "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  python -m pip install --no-cache-dir --user -r /opt/airflow/requirements.txt
fi

airflow db upgrade

if ! airflow users list | grep -q "admin"; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

exec airflow webserver