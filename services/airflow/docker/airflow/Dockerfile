FROM apache/airflow:2.2.3-python3.8

USER root

RUN apt update && \
    apt install -y git


RUN mkdir ../../libraries

RUN chown -R airflow:root /usr/local/src

RUN mkdir -p /opt/airflow/dbt/logs
RUN chown -R airflow /opt/airflow/dbt

USER airflow

RUN pip install --upgrade setuptools

# test if airflow is allowed to write to the /opt/airflow folder

ARG YOUR_ENV

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION=1.1.11

RUN pip install "poetry==$POETRY_VERSION"

RUN python3 --version

WORKDIR /opt/airflow/

ADD services/airflow/airflow.cfg services/airflow/poetry.lock services/airflow/pyproject.toml /opt/airflow/

# copy libs into the container, so their paths exist for editable installs
COPY libraries ../../libraries

# install airflow dependencies
RUN cd /opt/airflow && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

RUN pip install --upgrade setuptools

# copy dbt and dag files into the container
COPY services/airflow/src /opt/airflow/src
COPY services/dbt/* /opt/airflow/dbt/

# install dbt dependencies
RUN cd /opt/airflow/dbt && \
    poetry run dbt deps

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/src"