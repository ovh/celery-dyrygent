ARG PYTHON_VERSION='3'
FROM python:${PYTHON_VERSION}-slim

ARG CELERY_VERSION
ARG CELERY_BACKEND

ADD src/app /application
ADD src/celery_dyrygent /application/celery_dyrygent

WORKDIR /application
RUN pip3 install -U pip
RUN pip3 install PyMySQL[rsa]

RUN pip3 install celery[${CELERY_BACKEND}]${CELERY_VERSION}