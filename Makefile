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
	docker exec airflow-scheduler poetry run pytest -v -k 'not integration' -n 'auto'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

lint:
	docker exec airflow-scheduler poetry run flake8 
	
stylecheck:
	docker exec airflow-scheduler poetry run black --check

watch:
	docker exec -it airflow-scheduler poetry run python -m pytest -f --ignore ./logs -k 'not integration'

watchintegration:
	docker exec -it airflow-scheduler pytest -k 'integration' -n 'auto' -f

migrations:
	docker exec -w /opt/airflow/ airflow-scheduler poetry run yoyo apply --no-config-file --database postgresql://${TARGET_DB_USER}:${TARGET_DB_PW}@${TARGET_DB_HOSTNAME}/${TARGET_DB} ./dags/database/migrations/migration_files
