#!make
-include .env
export $(shell sed 's/=.*//' .env)

test:
	poetry run dbt --profiles-dir . test

build-docs:
	poetry run dbt --profiles-dir . docs generate

serve-docs:
	poetry run dbt --profiles-dir . docs serve

docs:
	make build-docs && make serve-docs

build:
	poetry run dbt deps

run:
	poetry run dbt --profiles-dir . run