FROM python:3.9-alpine

COPY requirements.txt /dbt/requirements.txt
WORKDIR /dbt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /dbt

