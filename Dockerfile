FROM apache/airflow:2.7.0
USER root
COPY requirements.txt /

RUN apt-get update

RUN apt-get install -y --fix-missing \
    freetds-dev \
    libkrb5-dev \
    libssl-dev \
    libffi-dev \
    python3-dev \
    iputils-ping \
    vim \
    nano 

RUN apt-get install -y \
    xvfb \
    libxi6 \
    libgconf-2-4 

RUN apt-get install -y postgresql-client

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt