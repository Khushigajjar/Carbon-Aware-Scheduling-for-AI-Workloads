FROM apache/airflow:2.8.1

USER root

RUN apt-get update && \
    apt-get install -y default-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir pyspark==3.4.0 elasticsearch==8.12.0 requests