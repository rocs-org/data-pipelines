setup:
	docker-compose up -d

down:
	docker-compose down -v

unittest:
	poetry run pytest -v -k 'not integration'

integrationtest: 
	docker exec airflow_worker pytest -v -k 'integration'

watch:
	docker exec airflow_sheduler poetry run ptw

lint:
	poetry run flake8 && poetry run black --check