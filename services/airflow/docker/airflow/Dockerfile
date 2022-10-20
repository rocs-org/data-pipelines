FROM apache/airflow:latest-python3.10

USER root

RUN apt update && \
    apt install -y git

#
#RUN mkdir ../../libraries
#
#RUN chown -R airflow:root /usr/local/src

# copy dbt files
COPY services/dbt /opt/airflow/dbt
RUN mkdir -p /opt/airflow/dbt/logs
RUN chown -R airflow /opt/airflow/dbt

USER airflow

ARG YOUR_ENV

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION=1.2.0

RUN pip install "poetry==$POETRY_VERSION"
RUN pip install --upgrade pip

WORKDIR /opt/airflow/

ADD services/airflow/airflow.cfg services/airflow/poetry.lock services/airflow/pyproject.toml /opt/airflow/

# copy libs into the container, so their paths exist for editable installs
COPY libraries ../../libraries

# install airflow dependencies
RUN cd /opt/airflow \
    && poetry config virtualenvs.create false \
    && poetry export -f requirements.txt --with dev --output requirements.txt \
    && grep -v "file:" requirements.txt > requirements_filtered.txt \
    && pip install -r requirements_filtered.txt

RUN pip install -e ../../libraries/clickhouse-helpers
RUN pip install -e ../../libraries/postgres-helpers

RUN pip install --upgrade setuptools

# copy dag files into the container
COPY services/airflow/src /opt/airflow/src

# install dbt dependencies
RUN cd /opt/airflow/dbt && \
    poetry run dbt deps

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/src"
