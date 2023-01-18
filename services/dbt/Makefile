#!make
-include .env
export $(shell sed 's/=.*//' .env)

install:
	 make build && poetry run pre-commit install

test:
	poetry run dbt --profiles-dir . test

build-docs:
	poetry run dbt --profiles-dir . docs generate

serve-docs:
	poetry run dbt --profiles-dir . docs serve --port 8000

docs:
	make build-docs && make serve-docs

build:
	poetry install && poetry run dbt deps

run:
	poetry run dbt --profiles-dir . run