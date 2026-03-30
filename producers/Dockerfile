FROM python:3.12-slim

WORKDIR /app

RUN pip install kafka-python faker --no-cache-dir

COPY loan_producer.py .

CMD ["python", "-u", "loan_producer.py"]
