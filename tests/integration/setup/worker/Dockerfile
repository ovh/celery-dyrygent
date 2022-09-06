ARG PYTHON_VERSION='3.8'
FROM python:${PYTHON_VERSION}-alpine

ARG CELERY_VERSION='5.2.7'

ADD src/app /application
ADD src/celery_dyrygent /application/celery_dyrygent

WORKDIR /application
RUN pip3 install celery[redis]==${CELERY_VERSION}
