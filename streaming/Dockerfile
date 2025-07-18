FROM python:3.11-slim

WORKDIR /app

COPY ./producer /app

RUN pip install --no-cache-dir pandas kafka-python

CMD ["python", "produce_csv.py"]
