#!make
include .env
export $(shell sed 's/=.*//' .env)

setup:
	docker-compose up -d && bash wait-for-healthy-container.sh clickhouse-db-testing 300

down:
	docker-compose down -v

unittest:
	poetry run pytest -v -n 'auto'

lint:
	poetry run pflake8 && make stylecheck
	
stylecheck:
	poetry run black --check .

watch:
	poetry run pytest -f

watchthis:
	poetry run pytest -f -k $(TEST)