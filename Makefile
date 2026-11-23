.DEFAULT_GOAL := help
COMPOSE ?= docker compose
DAYS ?= 1

help:           ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'


init:           ## Crée le fichier .env (à relire avant le 1er démarrage)
	@test -f .env && echo ".env existe déjà" || ( \
		cp .env.example .env && \
		sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u 2>/dev/null || echo 50000)/" .env && \
		echo ".env créé : changez les mots de passe !" \
	)

up:             ## Construit et démarre toute la stack
	$(COMPOSE) up -d --build
	@echo "Airflow : http://localhost:8080   API : http://localhost:8000/docs"

down:           ## Arrête la stack (les données sont conservées)
	$(COMPOSE) down

reset:          ## Arrête ET supprime les données (base, logs)
	$(COMPOSE) down -v

logs:           ## Suit les logs du scheduler
	$(COMPOSE) logs -f airflow-scheduler

backfill:       ## Charge l'historique : make backfill DAYS=7
	$(COMPOSE) exec airflow-scheduler airflow dags trigger crypto_backfill --conf '{"days": $(DAYS)}'

seed:           ## Recharge le référentiel des cryptos depuis config/coins.yaml
	$(COMPOSE) exec airflow-scheduler python -m crypto_pipeline.cli seed

check-coins:    ## Vérifie que coins.yaml est valide auprès de Binance et CoinGecko
	$(COMPOSE) exec airflow-scheduler python -m crypto_pipeline.cli check-coins

psql:           ## Ouvre une console PostgreSQL
	$(COMPOSE) exec postgres psql -U crypto -d crypto

test:           ## Lance les tests (démarre Postgres, crée la base de test)
	$(COMPOSE) up -d postgres
	@until $(COMPOSE) exec -T postgres pg_isready -U crypto >/dev/null 2>&1; do sleep 1; done
	@$(COMPOSE) exec -T postgres psql -U crypto -d postgres -c "CREATE DATABASE crypto_test" 2>/dev/null || true
	CRYPTO_TEST_DATABASE_URL=postgresql://crypto:$${POSTGRES_PASSWORD:-change-me-locally}@localhost:5432/crypto_test python -m pytest -v

lint:           ## Vérifie le style du code
	ruff check src dags tests scripts

.PHONY: help init up down reset logs backfill seed check-coins psql test lint
