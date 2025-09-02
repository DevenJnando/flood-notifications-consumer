FROM python:3.10.18
LABEL authors="jamesdev"
WORKDIR /
ENV PYTHONPATH="${PYTHONPATH}:/"
COPY requirements.txt /
WORKDIR /app
COPY ./app /app
WORKDIR /
RUN pip install -r /requirements.txt
CMD python3 ./app/main.py