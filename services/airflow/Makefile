#!make
-include .env
export $(shell sed 's/=.*//' .env)

test:
	env

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d && make migrations && bash wait-for-healthy-container.sh airflow-scheduler 300

down:
	docker-compose down -v

unittest:
	docker exec airflow-scheduler pytest -v -k 'not integration'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

lint:
	poetry run pflake8 && poetry run black --check .

watch:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k 'not integration'

watchthis:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k $(TEST)

migrations:
	docker exec -w /opt/airflow/ airflow-scheduler poetry run yoyo apply --no-config-file --database postgresql://${TARGET_DB_USER}:${TARGET_DB_PW}@${TARGET_DB_HOSTNAME}/${TARGET_DB} ../../libraries/database/database/migrations/migration_files
