# Driver Spark : JupyterLab sur la meme image que le Master et les Workers
FROM apache/spark:3.5.3-python3

USER root

# PySpark vient de /opt/spark (meme version que le cluster)
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3

RUN pip3 install --no-cache-dir \
    jupyterlab \
    pandas \
    pyarrow \
    matplotlib \
    kafka-python \
    importlib_metadata \
 && pip3 install --no-cache-dir --no-deps delta-spark==3.2.0

WORKDIR /workspace