#!make
include .env
export $(shell sed 's/=.*//' .env)

test:
	env

build:
	docker-compose build

setup:
	docker-compose up -d && make migrations

down:
	docker-compose down -v

unittest:
	docker exec airflow-scheduler poetry run pytest -v -k 'not integration'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

lint:
	docker exec airflow-scheduler poetry run flake8 
	
stylecheck:
	docker exec airflow-scheduler poetry run black --check

watch:
	docker exec -w /opt/airflow/dags -it airflow-scheduler poetry run python -m pytest -k 'not integration' -f --ignore ./logs

migrations:
	docker exec -w /opt/airflow/ airflow-scheduler poetry run yoyo apply --database postgresql://${TARGET_DB_USER}:${TARGET_DB_PW}@${TARGET_DB_HOSTNAME}/${TARGET_DB} ./dags/database/migrations
