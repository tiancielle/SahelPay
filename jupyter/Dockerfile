# Driver Spark : JupyterLab + PySpark 3.5.3
# Python 3.10 et Java 11 = memes versions que l'image apache/spark:3.5.3-python3 (workers)
FROM python:3.10-slim-bullseye

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-11-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.0 \
    jupyterlab \
    pandas \
    pyarrow \
    matplotlib \
    kafka-python

WORKDIR /workspace
