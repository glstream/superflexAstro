FROM quay.io/astronomer/astro-runtime:5.0.3


ENV AIRFLOW_CONN_POSTGRES_DOCKER='postgres://dynasty1:superflex1%21@dynasty.postgres.database.azure.com/postgres'