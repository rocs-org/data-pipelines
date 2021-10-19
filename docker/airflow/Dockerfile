FROM apache/airflow:2.1.1rc1-python3.8

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN chown -R airflow:airflow /usr/local/src

USER airflow

ARG YOUR_ENV

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION=1.1.11

RUN pip install "poetry==$POETRY_VERSION"

RUN python3 --version

WORKDIR /opt/airflow/

ADD airflow.cfg poetry.lock pyproject.toml /opt/airflow/

RUN pip install --requirement <(poetry export  --without-hashes --dev --format requirements.txt)

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags"