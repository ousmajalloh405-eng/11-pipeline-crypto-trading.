#!/bin/bash
# Exécuté automatiquement au premier démarrage du conteneur Postgres.
# Crée la base de métadonnées d'Airflow, séparée de la base de données métier (crypto).
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE airflow;
EOSQL
