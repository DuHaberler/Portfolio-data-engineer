#!/bin/bash

set -e

psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "/opt/postgresql/schema/001_create_orders.sql"

psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "/opt/postgresql/schema/002_create_payments.sql"

psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "/opt/postgresql/schema/003_create_refunds.sql"

psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -v debezium_user_password="$DEBEZIUM_USER_PASSWORD" \
  -f "/opt/postgresql/schema/006_create_debezium_user.sql"

psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "/opt/postgresql/schema/004_create_publications.sql"

  psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "/opt/postgresql/schema/005_create_replication_slots.sql"

psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "/opt/postgresql/seed/004_seed_data.sql"

