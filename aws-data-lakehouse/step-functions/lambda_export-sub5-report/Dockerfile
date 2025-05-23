ARG BASE_IMAGE=amazon/aws-lambda-python:3.9
FROM $BASE_IMAGE

WORKDIR  /var/task/
COPY requirements.txt /var/task/requirements.txt
# Install dependencies for function
RUN pip install -r /var/task/requirements.txt

COPY lambda_handler.py /var/task/lambda_handler.py