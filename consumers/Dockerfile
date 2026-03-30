FROM python:3.12-slim

WORKDIR /app

RUN pip install kafka-python boto3 faker --no-cache-dir

COPY loan_consumer.py .

CMD ["python", "-u", "loan_consumer.py"]
