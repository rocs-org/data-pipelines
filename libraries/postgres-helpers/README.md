# database

Database schemata and access helpers for the research PostgresQL database at [ROCS](https://rocs.hu-berlin.de/)

This project uses [poetry](https://python-poetry.org/) for package management.

## How to work with this project:

Prerequisites: 
1. Install poetry: `$pip install --user poetry`
2. Set up [docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/) for your operating system

Usage:
4. Run `make build` then `make  setup`
5. Run tests: `$make watch` for continuous testing during development.


## Contribute:

To contribute to this project please do the following:
1. create a feature branch: `git checkout -b feature/YOUR-FEATURE-NAME`,
2. develop your feature while making sure that tests and linting are still passing,
3. push your feature branch to the repo `git push -u origin feature/YOUR-FEATURE-NAME` and open a pull request.
4. Ask one of the maintainers @davhin and @jakobkolb to review.