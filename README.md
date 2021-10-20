# data-pipelines

Data pipelines and infrastructure to reliably provide high quality data for research at [ROCS](https://rocs.hu-berlin.de/)

Written in python, built with [Airflow](https://airflow.apache.org/) and Docker.

This project uses [poetry](https://python-poetry.org/) for package management and [git-secret](https://git-secret.io/) to manage secrets.

## How to work with this project:

Prerequisites: 
1. Set up [docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/) for your operating system
2. Set up [git secret](https://git-secret.io/installation)
3. Contact @jakobkolb or @davhin to get accesss to repository secrets

Usage:
1. Clone the project
2. Install poetry: `$pip install --user poetry`
3. run git-secret reveal
4. Run `make build` then `make  setup`
5. Run tests: `$make watch` or open the Airflow UI on `localhost:8080` and login with username: `airflow` and password: `airflow`

## Contribute:

To contribute to this project please do the following:
2. create a feature branch: `git checkout -b feature/YOUR-FEATURE-NAME`,
3. develop your feature while making sure that tests and linting are still passing,
4. push your feature branch to the repo `git push -u origin feature/YOUR-FEATURE-NAME` and open a pull request.
5. Ask one of the maintainers @davhin and @jakobkolb to review.
