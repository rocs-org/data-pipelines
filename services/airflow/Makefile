#!make
-include .env
export $(shell sed 's/=.*//' .env)

test:
	env

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d && \
	bash wait-for-healthy-container.sh airflow-scheduler 300 && \
	make migrations

dev:
	docker-compose -f docker-compose.yaml -f docker-compose.dev.yaml up -d && \
	bash wait-for-healthy-container.sh airflow-scheduler 300 && \
	make migrations

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

this:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k $(test)

postgres_migrations:
	docker exec -w /opt/airflow/ airflow-scheduler poetry run yoyo apply --no-config-file --database postgresql://${TARGET_DB_USER}:${TARGET_DB_PW}@${TARGET_DB_HOSTNAME}/${TARGET_DB} ../../libraries/postgres-helpers/postgres_helpers/migrations/migration_files

clickhouse_migrations:
	docker exec airflow-scheduler poetry run migrate-clickhouse --host ${CLICKHOUSE_HOSTNAME} --port ${CLICKHOUSE_PORT} --database ${CLICKHOUSE_DB} --user ${CLICKHOUSE_USER} --password ${CLICKHOUSE_PASSWORD}

migrations:
	make postgres_migrations && make clickhouse_migrations
