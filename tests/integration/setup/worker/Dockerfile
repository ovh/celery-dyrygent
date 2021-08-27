FROM python:3.6-alpine


ADD src/app /application
ADD src/requirements.txt /application

WORKDIR /application
RUN pip3 install -r requirements.txt