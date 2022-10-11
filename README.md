# data-pipelines

[![tests](https://github.com/rocs-org/data-pipelines/actions/workflows/airflow_jobs.yml/badge.svg)](https://github.com/rocs-org/data-pipelines/actions/workflows/airflow_jobs.yml) [![Publish Docker image](https://github.com/rocs-org/data-pipelines/actions/workflows/publish_docker.yml/badge.svg?branch=main)](https://github.com/rocs-org/data-pipelines/actions/workflows/publish_docker.yml) 

Monorepo that contains data processing related libraries and services developed at [ROCS](https://rocs.hu-berlin.de/)

Data model documentation is at [rocs-dbt-docs.netlify.app](https://rocs-dbt-docs.netlify.app)

This project uses [poetry](https://python-poetry.org/) for package management and [git-secret](https://git-secret.io/) to manage secrets.

## Contribute:

To contribute to this project please do the following:
1. create a feature branch: `git checkout -b feature/YOUR-FEATURE-NAME`,
3. develop your feature while making sure that tests and linting are still passing,
3. To add a library or service enter the libraries or services folder and run `$poetry new name_of_the_new_lib_or_service`
4. push your feature branch to the repo `git push -u origin feature/YOUR-FEATURE-NAME` and open a pull request.
5. Ask one of the maintainers @davhin and @jakobkolb to review.
