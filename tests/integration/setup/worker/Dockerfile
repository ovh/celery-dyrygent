ARG PYTHON_VERSION='3.6'
FROM python:${PYTHON_VERSION}-alpine

ARG CELERY_VERSION='4.2.0'

ADD src/app /application
ADD src/celery_dyrygent /application/celery_dyrygent

WORKDIR /application
RUN pip3 install celery[redis]==${CELERY_VERSION}