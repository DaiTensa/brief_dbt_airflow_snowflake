FROM quay.io/astronomer/astro-runtime:12.10.0 

COPY ./requirements.txt /tmp/requirements.txt

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt && \
    pip install --no-cache-dir dbt-snowflake
