lint:
	poetry run pflake8 . && poetry run black --check . && poetry run mypy .

watch:
	poetry run pytest -f

unittest:
	poetry run pytest