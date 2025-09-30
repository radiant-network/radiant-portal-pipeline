FROM --platform=$BUILDPLATFORM apache/airflow:2.10.5-python3.12

USER airflow

RUN mkdir -p /home/airflow/.venv/radiant

RUN python3 -m venv /home/airflow/.venv/radiant

COPY requirements-airflow.txt /home/airflow/.venv/radiant/requirements-airflow.txt
RUN /home/airflow/.venv/radiant/bin/pip install --no-deps -r /home/airflow/.venv/radiant/requirements-airflow.txt





