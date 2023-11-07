DOCKER=$(shell which docker)
DOCKER-COMPOSE=$(shell which docker-compose)

help:
	@grep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

init: ## run database migrations and create the first user account
	$(DOCKER-COMPOSE) up airflow-init

up: ## start all services
	$(DOCKER-COMPOSE) up --detach

down: ## stop all services
	$(DOCKER-COMPOSE) down

clean: ## stop and delete containers, delete volumes with database data and download images
	$(DOCKER-COMPOSE) down --volumes --rmi all

restart: ## stop and start containers
	$(DOCKER-COMPOSE) down && $(DOCKER-COMPOSE) up --detach

reset: ## stops and restarts all services with a cache clearing step
	$(DOCKER-COMPOSE) down && $(DOCKER-COMPOSE) down --volumes --rmi all && $(DOCKER-COMPOSE) up --detach
