# Dockerfile to build docker image used to test my PySpark client

# from https://hub.docker.com/r/jupyter/all-spark-notebook
FROM jupyter/all-spark-notebook:latest
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH

WORKDIR /home/jovyan/work/

RUN conda install -c anaconda pytest
