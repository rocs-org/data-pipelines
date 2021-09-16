FROM apache/airflow:2.1.1rc1-python3.8

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN chown -R airflow:airflow /usr/local/src

USER airflow

ARG YOUR_ENV

ENV YOUR_ENV=${YOUR_ENV} \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION=1.0.0 \
    POETRY_VIRTUALENVS_CREATE=false

RUN pip install "poetry==$POETRY_VERSION"

RUN python3 --version

WORKDIR /opt/airflow/

ADD airflow.cfg poetry.lock pyproject.toml .flake8 /opt/airflow/

RUN poetry install $(test "$YOUR_ENV" == production && echo "--no-dev") --no-interaction --no-ansi

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags"