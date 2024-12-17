# Variables
DOCKER_COMPOSE = docker compose

# Lancer les services
start:
	$(DOCKER_COMPOSE) up --build

# Arrêter les services
stop:
	$(DOCKER_COMPOSE) down

# Afficher les logs
logs:
	$(DOCKER_COMPOSE) logs -f

# Exécuter un backfill
backfill:
	docker exec -it simple-etl-airflow-webserver-1 airflow dags backfill -s 2024-12-16 -e 2024-12-17 simple_etl_pipeline

# Nettoyer les fichiers de sortie
clean:
	rm -rf data/json_data/* data/html_data/*

# Recréer les services
rebuild:
	$(DOCKER_COMPOSE) down --volumes
	$(DOCKER_COMPOSE) build
	$(DOCKER_COMPOSE) up -d

.PHONY: start stop logs backfill clean rebuild
