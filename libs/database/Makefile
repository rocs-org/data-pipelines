#!make
include .env
export $(shell sed 's/=.*//' .env)

test:
	env

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d

down:
	docker-compose down -v

unittest:
	poetry run pytest -v -n 'auto'

lint:
	poetry run flake8
	
stylecheck:
	poetry run black --check

watch:
	poetry run pytest -f