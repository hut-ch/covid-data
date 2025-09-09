#!/bin/bash
set -e

echo $POSTGRES_USER
echo $POSTGRES_DB
echo $AIRFLOW_DB
echo $AIRFLOW_DB_USER
echo $AIRFLOW_DB_PW
echo $METABASE_DB
echo $METABASE_DB_USER
echo $AMETABASE_DB_PW
echo $PWD

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $AIRFLOW_DB_USER WITH PASSWORD '$AIRFLOW_DB_PW';
    CREATE DATABASE $AIRFLOW_DB OWNER $AIRFLOW_DB_USER;
    GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB TO $AIRFLOW_DB_USER;
    CREATE USER $METABASE_DB_USER WITH PASSWORD '$METABASE_DB_PW';
    CREATE DATABASE $METABASE_DB OWNER $METABASE_DB_USER;
    GRANT ALL PRIVILEGES ON DATABASE $METABASE_DB TO $METABASE_DB_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$AIRFLOW_DB" <<-EOSQL
    GRANT ALL ON SCHEMA public TO $AIRFLOW_DB_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$METABASE_DB" <<-EOSQL
    GRANT ALL ON SCHEMA public TO $METABASE_DB_USER;
EOSQL

if [ -f /backup/metabase_db_backup.sql ]; then
    echo "Restoring Metabase database..."
    psql --username "$POSTGRES_USER" --dbname "$METABASE_DB" < /backup/metabase_db_backup.sql
else
    echo "Backup file not found at /backup/metabase_db_backup.sql"
    exit 1
fi
