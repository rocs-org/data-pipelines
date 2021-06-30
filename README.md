# data-pipelines

Data pipelines and infrastructure to reliably provide high quality data for research at [ROCS](https://rocs.hu-berlin.de/)

Written in python, built with [Airflow](https://airflow.apache.org/) and Docker.

This project uses [poetry](https://python-poetry.org/) for package management.

## How to work with this project:

1. Install poetry: `$pip install --user poetry`
2. Install and set up docker and docker-compose 
3. Install dependencies: `poetry install`
4. Spin up containers: `docker-compose up -d`
5. Run tests: `docker exec airflow_sheduler poetry run pytest -v`

## Contribute:

To contribute to this project please do the following:
1. create a feature branch: `git checkout -b feature/YOUR-FEATURE-NAME`,
2. develop your feature while making sure that tests and linting are still passing,
3. push your feature branch to the repo `git push -u origin feature/YOUR-FEATURE-NAME` and open a pull request.
4. Ask one of the maintainers @davhin and @jakobkolb to review.