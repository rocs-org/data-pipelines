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
	poetry run pytest -v -k 'not integration' -n 'auto'

lint:
	poetry run flake8
	
stylecheck:
	poetry run black --check

watch:
	poetry run pytest -f

migrations:
	poetry run yoyo apply --no-config-file --database postgresql://${TARGET_DB_USER}:${TARGET_DB_PW}@${TARGET_DB_HOSTNAME}/${TARGET_DB} ./dags/database/migrations/migration_files
