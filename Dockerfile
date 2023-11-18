FROM apache/airflow:2.7.3-python3.10

# install base dependencies
COPY ./requirements/requirements.txt .
RUN pip install -r requirements.txt

## create virtual environments for google ads and cosmos
RUN export PIP_USER=false && python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.7.1 && deactivate && export PIP_USER=true

# Add the dags directory to the PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW__CORE__DAGS_FOLDER}"