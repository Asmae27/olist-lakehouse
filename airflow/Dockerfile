FROM apache/airflow:3.3.2

USER root
COPY --from=docker:28.5.1-cli /usr/local/bin/docker /usr/local/bin/docker
USER airflow