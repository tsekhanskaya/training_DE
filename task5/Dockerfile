FROM jupyter/pyspark-notebook

USER root

# Установка PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar \
    && mv postgresql-42.2.23.jar /opt/conda/share/jupyter/kernels/python3/

USER $NB_UID
