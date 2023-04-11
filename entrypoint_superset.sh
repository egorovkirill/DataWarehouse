#!/bin/bash

set -e

# Create admin user
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Superset}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}"

# Install clickhouse-driver
pip install clickhouse-connect

# Migrate DB and initialize
superset db upgrade
superset init

exec gunicorn \
  --timeout "${GUNICORN_TIMEOUT:-60}" \
  --bind "0.0.0.0:${SUPERSET_PORT:-8088}" \
  --access-logfile "-" \
  "superset.app:create_app()"