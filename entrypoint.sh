#!/bin/bash

if [ "$_AIRFLOW_WWW_USER_CREATE" = "true" ]; then
    airflow users create \
        --role Admin \
        --username "${_AIRFLOW_WWW_USER_USERNAME}" \
        --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
        --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Airflow}" \
        --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-User}" \
        --email "${_AIRFLOW_WWW_USER_EMAIL:-noreply@example.com}"
fi

exec "$@"