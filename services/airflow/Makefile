#!make
include .env
export $(shell sed 's/=.*//' .env)

test:
	env

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d && make migrations

down:
	docker-compose down -v

unittest:
	docker exec airflow-scheduler pytest -v -k 'not integration' -n 'auto'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

lint:
	poetry run flake8

watch:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k 'not integration'

migrations:
	docker exec -w /opt/airflow/ airflow-scheduler poetry run yoyo apply --no-config-file --database postgresql://${TARGET_DB_USER}:${TARGET_DB_PW}@${TARGET_DB_HOSTNAME}/${TARGET_DB} ../../libraries/database/migrations/migration_files
