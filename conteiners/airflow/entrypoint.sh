#!/bin/bash
set -e

echo ">>> Iniciando configuración inicial de Airflow..."

# Inicializa la DB solo si NO existe
if [ ! -f "/opt/airflow/airflow.db" ]; then
    echo ">>> Base de datos NO existe, inicializando..."
    airflow db init
else
    echo ">>> Base de datos ya existe, saltando airflow db init"
fi

# Crea usuario admin solo si NO existe
if ! airflow users list | grep -q "admin"; then
    echo ">>> Usuario admin NO existe, creándolo..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
else
    echo ">>> Usuario admin ya existe, saltando creación"
fi

echo ">>> Comprobando pool 'default_pool'..."
# Si el pool default_pool no existe, lo creamos con 32 slots
if ! airflow pools get default_pool >/dev/null 2>&1; then
  echo ">>> Creando pool 'default_pool'..."
  airflow pools set default_pool 32 "Default pool"
fi

echo ">>> Iniciando webserver y scheduler..."
airflow webserver --port 8080 &

airflow scheduler
