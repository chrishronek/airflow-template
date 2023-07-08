FROM apache/airflow:2.5.1-python3.10

# install base dependencies
COPY ./requirements/requirements.txt .
RUN pip install -r requirements.txt

