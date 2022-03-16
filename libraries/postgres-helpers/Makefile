#!make
include .env
export $(shell sed 's/=.*//' .env)

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d && bash wait-for-healthy-container.sh db 30

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