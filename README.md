# data-pipelines

Data pipelines and infrastructure to reliably provide high quality data for research at [ROCS](https://rocs.hu-berlin.de/)

Written in python, built with [Airflow](https://airflow.apache.org/) and Docker.

This project uses [poetry](https://python-poetry.org/) for package management and [git-secret](https://git-secret.io/) to manage secrets.

## How to work with this project:

Prerequisites: 
1. Set up [docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/) for your operating system
2. Set up [git secret](https://git-secret.io/installation)
3. Make yourself acquainted with the architecture of this airflow setup consisting of a 
   webserver, sheduler, queue, workers and database for metadata as outlined [here](https://airflow.apache.org/docs/apache-airflow/stable/executor/celery.html#architecture)
4. Contact @jakobkolb or @davhin to get accesss to repository secrets

Usage:
1. Clone the project
2. Install poetry: `$pip install --user poetry`
3. run git-secret reveal
4. Run `make build` then `make  setup`
5. Run tests: `$make watch` or open the Airflow UI on `localhost:8080` and login with username: `airflow` and password: `airflow`


## Contribute:

To contribute to this project please do the following:
1. create a feature branch: `git checkout -b feature/YOUR-FEATURE-NAME`,
2. develop your feature while making sure that tests and linting are still passing,
3. push your feature branch to the repo `git push -u origin feature/YOUR-FEATURE-NAME` and open a pull request.
4. Ask one of the maintainers @davhin and @jakobkolb to review.

## Pipeline Development:
1. Copy and rename an existing template pipeline (the entire folder) such as [`csv_download_to_postgres`](https://github.com/rocs-org/data-pipelines/tree/main/dags/csv_download_to_postgres)
2. If you plan changing or creating tables in the target DB, add the necessary [migration](https://en.wikipedia.org/wiki/Schema_migration) files in `dags/database/migrations/migration_files` 
   and consider writing some tests in `dags/database/migrations/migration_tests` in case the migrations are not trivial.
3. Tests for this pipeline are in the same folder (unit tests for single functions and an integration test that 
   executes the entire dag and checks whether the resulting data in the db matches the expected results). 
   Adapt these tests to assert the target behavior of your pipeline (and ideally also its parts, as this helps a LOT with debugging)
4. If your pipeline downloads files from somewhere, place a shortened copy of such files in `files/static`. 
   Files from this folder are available at `http://static-files/static/YOURFILE.NAME`. 
   Use `execute_dag` and `if_var_exists_in_dag_conf_use_as_first_arg` helper 
   from `dags.helpers.test_helpers` to [inject](https://en.wikipedia.org/wiki/Dependency_injection) this url into the environment on the worker that executes your dag during 
   integration testing.
5. To test against a test database, use the `db_context` [test fixture](https://docs.pytest.org/en/6.2.x/fixture.html#what-fixtures-are). This fixture provides you with a clean database 
   with a random name that has all the migrations applied. Also, this fixture sets an environment variable 
   called `TARGET_DB` with this databases name. Helper functions such as `connect_to_db_and_insert_pandas_dataframe` 
   use this environment variable to connect to the database so in the context of unit tests. 
   To set this environment variable on the worker node during integration tests (to make sure that database helpers connect to the test database), 
   you have to use the `execute_dag` and `set_env_variable_from_dag_config_if_present` helpers in `dags.helpers.test_helpers`. 