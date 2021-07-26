setup:
	docker-compose up -d

down:
	docker-compose down -v

unittest:
	docker exec airflow-scheduler poetry run pytest -v -k 'not integration'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

watch:
	docker exec -w /opt/airflow/dags airflow-scheduler poetry run python -m pytest -k 'not integration' -f --ignore ./logs

lint:
	poetry run flake8 && poetry run black --check