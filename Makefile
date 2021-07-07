test:
	docker exec airflow_sheduler pytest -v

watch:
	docker exec airflow_sheduler poetry run ptw

lint:
	poetry run flake8 && poetry run black --check